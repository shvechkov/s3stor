package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	defaultBlockSize  = 1024 * 1024 // 1 MB
	defaultCatalogKey = "catalog.json"
	lockTTLSeconds    = 300             // 5 minutes TTL for locks
	maxLockRetries    = 3               // Maximum lock acquisition retries
	retryBaseDelay    = 1 * time.Second // Base delay for exponential backoff
)

type S3ProviderConfig struct {
	Provider        string
	BucketName      string
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	CatalogKey      string
	BlockSize       int
	ForcePathStyle  bool
}

type FileMap struct {
	FileName  string   `json:"file_name"`
	FileSize  int64    `json:"file_size"`
	BlockSize int      `json:"block_size"`
	Blocks    []string `json:"blocks"`
}

type CatalogEntry struct {
	FileName string `json:"file_name"`
	FileSize int64  `json:"file_size"`
	MapKey   string `json:"map_key"`
}

type SnapshotCatalog struct {
	SnapshotID string         `json:"snapshot_id"`
	Timestamp  time.Time      `json:"timestamp"`
	ComputerID string         `json:"computer_id"`
	Files      []CatalogEntry `json:"files"`
}

func main() {
	providerConfig := S3ProviderConfig{
		Provider:        "aws",
		BucketName:      os.Getenv("S3_BUCKET"),
		Endpoint:        os.Getenv("S3_ENDPOINT"),
		Region:          os.Getenv("S3_REGION"),
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		CatalogKey:      defaultCatalogKey,
		BlockSize:       defaultBlockSize,
		ForcePathStyle:  false,
	}

	if provider := os.Getenv("S3_PROVIDER"); provider != "" {
		providerConfig.Provider = provider
	}
	if providerConfig.Provider == "wasabi" {
		providerConfig.ForcePathStyle = true
		if providerConfig.Region == "" {
			providerConfig.Region = "us-east-1"
		}
	}
	if providerConfig.BucketName == "" {
		log.Fatal("S3_BUCKET environment variable is required")
	}
	if providerConfig.Region == "" {
		log.Fatal("S3_REGION environment variable is required")
	}
	if providerConfig.AccessKeyID == "" || providerConfig.SecretAccessKey == "" {
		log.Fatal("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables are required")
	}

	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <sync|ls|get|map|snapshot|delete-snapshot|cleanup-blocks|delete> [args...]")
		os.Exit(1)
	}

	cmd := os.Args[1]
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(providerConfig.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			providerConfig.AccessKeyID,
			providerConfig.SecretAccessKey,
			"",
		)),
		config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				if service == s3.ServiceID && providerConfig.Endpoint != "" {
					return aws.Endpoint{
						URL:               providerConfig.Endpoint,
						SigningRegion:     providerConfig.Region,
						HostnameImmutable: providerConfig.ForcePathStyle,
					}, nil
				}
				return aws.Endpoint{}, fmt.Errorf("unknown endpoint requested")
			}),
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = providerConfig.ForcePathStyle
	})
	uploader := manager.NewUploader(client)

	switch cmd {
	case "sync":
		if len(os.Args) < 3 {
			fmt.Println("Usage: sync <file_or_dir>")
			os.Exit(1)
		}
		path := os.Args[2]
		info, err := os.Stat(path)
		if err != nil {
			log.Fatal(err)
		}
		if info.IsDir() {
			syncDir(ctx, client, uploader, path, providerConfig)
		} else {
			baseDir := filepath.Dir(path)
			if err := syncFile(ctx, client, uploader, path, baseDir, providerConfig); err != nil {
				log.Fatalf("Failed to sync file %s: %v", path, err)
			}
		}
	case "ls":
		if len(os.Args) == 3 {
			snapshotID := os.Args[2]
			listSnapshotFiles(ctx, client, snapshotID, providerConfig)
		} else {
			listFiles(ctx, client, providerConfig)
		}
	case "get":
		if len(os.Args) < 4 || len(os.Args) > 5 {
			fmt.Println("Usage: get [<snapshot_id>] <file_name> <output_dir>")
			os.Exit(1)
		}
		var snapshotID, fileName, outDir string
		if len(os.Args) == 5 {
			snapshotID = os.Args[2]
			fileName = os.Args[3]
			outDir = os.Args[4]
		} else {
			fileName = os.Args[2]
			outDir = os.Args[3]
		}
		getFile(ctx, client, snapshotID, fileName, outDir, providerConfig)
	case "map":
		if len(os.Args) < 3 {
			fmt.Println("Usage: map <file_name>")
			os.Exit(1)
		}
		fileName := os.Args[2]
		getFileMap(ctx, client, fileName, providerConfig)
	case "snapshot":
		if len(os.Args) < 4 {
			fmt.Println("Usage: snapshot <dir> <snapshot_id> [changed_files...]")
			os.Exit(1)
		}
		dir := os.Args[2]
		snapshotID := os.Args[3]
		changedFiles := os.Args[4:]
		hostname, err := os.Hostname()
		if err != nil {
			log.Printf("Failed to get hostname: %v, using 'unknown'", err)
			hostname = "unknown"
		}
		uniqueSnapshotID := fmt.Sprintf("%s-%s", hostname, snapshotID)
		createSnapshot(ctx, client, uploader, dir, uniqueSnapshotID, changedFiles, providerConfig)
	case "delete-snapshot":
		if len(os.Args) != 3 {
			fmt.Println("Usage: delete-snapshot <snapshot_id>")
			os.Exit(1)
		}
		snapshotID := os.Args[2]
		deleteSnapshot(ctx, client, snapshotID, providerConfig)
	case "cleanup-blocks":
		hostname, err := os.Hostname()
		if err != nil {
			log.Printf("Failed to get hostname: %v, using 'unknown'", err)
			hostname = "unknown"
		}
		cleanupBlocks(ctx, client, hostname, providerConfig)
	case "delete":
		if len(os.Args) != 3 {
			fmt.Println("Usage: delete <file_name>")
			os.Exit(1)
		}
		fileName := os.Args[2]
		hostname, err := os.Hostname()
		if err != nil {
			log.Printf("Failed to get hostname: %v, using 'unknown'", err)
			hostname = "unknown"
		}
		deleteFile(ctx, client, hostname, fileName, providerConfig)
	default:
		fmt.Println("Unknown command:", cmd)
		os.Exit(1)
	}
}

func syncDir(ctx context.Context, client *s3.Client, uploader *manager.Uploader, root string, cfg S3ProviderConfig) {
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			fmt.Println("Syncing:", path)
			if err := syncFile(ctx, client, uploader, path, root, cfg); err != nil {
				log.Printf("Failed to sync file %s: %v", path, err)
			}
		}
		return nil
	})
	if err != nil {
		log.Println("Directory walk error:", err)
	}
}

func syncFile(ctx context.Context, client *s3.Client, uploader *manager.Uploader, fullPath, baseDir string, cfg S3ProviderConfig) error {
	relPath, err := filepath.Rel(baseDir, fullPath)
	if err != nil {
		relPath = filepath.Base(fullPath)
	}
	f, err := os.Open(fullPath)
	if err != nil {
		return fmt.Errorf("open error: %w", err)
	}
	defer f.Close()

	fileInfo, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat error: %w", err)
	}

	var blocks []string
	buf := make([]byte, cfg.BlockSize)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			blockHash := hashBlock(buf[:n])
			if err := uploadBlock(ctx, client, blockHash, buf[:n], cfg); err != nil {
				return fmt.Errorf("upload block %s error: %w", blockHash, err)
			}
			blocks = append(blocks, blockHash)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read error: %w", err)
		}
	}

	fm := FileMap{
		FileName:  relPath,
		FileSize:  fileInfo.Size(),
		BlockSize: cfg.BlockSize,
		Blocks:    blocks,
	}

	mapKey := fmt.Sprintf("maps/%s.json", relPath)
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("Failed to get hostname for lock: %v, using 'unknown'", err)
		hostname = "unknown"
	}
	lockKey := fmt.Sprintf("locks/global/%s.lock", relPath)
	if err := acquireLock(ctx, client, lockKey, hostname, cfg); err != nil {
		return fmt.Errorf("failed to acquire lock for %s: %w", relPath, err)
	}
	defer releaseLock(ctx, client, lockKey, cfg)

	mapJSON, err := json.Marshal(fm)
	if err != nil {
		return fmt.Errorf("json marshal error: %w", err)
	}
	if err := putObj(ctx, client, mapKey, mapJSON, cfg); err != nil {
		return fmt.Errorf("put map object error: %w", err)
	}

	if err := updateCatalog(ctx, client, fm, mapKey, cfg); err != nil {
		return fmt.Errorf("update catalog error: %w", err)
	}

	fmt.Printf("Synced %s (%d bytes)\n", relPath, fileInfo.Size())
	return nil
}

func hashBlock(data []byte) string {
	h := sha256.Sum256(data)
	return fmt.Sprintf("%x", h)
}

func uploadBlock(ctx context.Context, client *s3.Client, hash string, data []byte, cfg S3ProviderConfig) error {
	key := "blocks/" + hash
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(cfg.BucketName),
		Key:    aws.String(key),
	})
	if err == nil {
		fmt.Println("Block exists:", hash)
		return nil
	}
	if err := putObj(ctx, client, key, data, cfg); err != nil {
		return fmt.Errorf("put block object error: %w", err)
	}
	fmt.Println("Uploaded block:", hash)
	return nil
}

func putObj(ctx context.Context, client *s3.Client, key string, data []byte, cfg S3ProviderConfig) error {
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.BucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("put object error for key %s: %w", key, err)
	}
	return nil
}

func acquireLock(ctx context.Context, client *s3.Client, lockKey, owner string, cfg S3ProviderConfig) error {
	for attempt := 1; attempt <= maxLockRetries; attempt++ {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:   aws.String(cfg.BucketName),
			Key:      aws.String(lockKey),
			Body:     bytes.NewReader([]byte(owner)),
			Metadata: map[string]string{"created": time.Now().UTC().Format(time.RFC3339)},
		})
		if err == nil {
			return nil
		}

		// Check if lock exists
		obj, headErr := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(cfg.BucketName),
			Key:    aws.String(lockKey),
		})
		if headErr == nil {
			// Lock exists, check if expired
			createdStr, exists := obj.Metadata["created"]
			if exists {
				created, parseErr := time.Parse(time.RFC3339, createdStr)
				if parseErr == nil && time.Since(created) > lockTTLSeconds*time.Second {
					// Lock expired, delete it
					_, delErr := client.DeleteObject(ctx, &s3.DeleteObjectInput{
						Bucket: aws.String(cfg.BucketName),
						Key:    aws.String(lockKey),
					})
					if delErr == nil {
						continue // Retry acquiring the lock
					}
					log.Printf("Failed to delete expired lock %s: %v", lockKey, delErr)
				}
			}
			// Lock is held, wait and retry
			delay := retryBaseDelay * time.Duration(1<<uint(attempt-1))
			log.Printf("Lock %s held, retrying after %v (attempt %d/%d)", lockKey, delay, attempt, maxLockRetries)
			time.Sleep(delay)
			continue
		}
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	return fmt.Errorf("failed to acquire lock after %d attempts", maxLockRetries)
}

func releaseLock(ctx context.Context, client *s3.Client, lockKey string, cfg S3ProviderConfig) error {
	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(cfg.BucketName),
		Key:    aws.String(lockKey),
	})
	if err != nil {
		log.Printf("Failed to release lock %s: %v", lockKey, err)
	}
	return nil
}

func updateCatalog(ctx context.Context, client *s3.Client, fm FileMap, mapKey string, cfg S3ProviderConfig) error {
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("Failed to get hostname for lock: %v, using 'unknown'", err)
		hostname = "unknown"
	}
	lockKey := "locks/global/catalog.lock"
	if err := acquireLock(ctx, client, lockKey, hostname, cfg); err != nil {
		return fmt.Errorf("failed to acquire catalog lock: %w", err)
	}
	defer releaseLock(ctx, client, lockKey, cfg)

	var catalog []CatalogEntry
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.BucketName),
		Key:    aws.String(cfg.CatalogKey),
	})
	if err == nil {
		defer out.Body.Close()
		if err := json.NewDecoder(out.Body).Decode(&catalog); err != nil {
			return fmt.Errorf("catalog decode error: %w", err)
		}
	}

	newCatalog := []CatalogEntry{}
	for _, entry := range catalog {
		if entry.FileName != fm.FileName {
			newCatalog = append(newCatalog, entry)
		}
	}
	newCatalog = append(newCatalog, CatalogEntry{
		FileName: fm.FileName,
		FileSize: fm.FileSize,
		MapKey:   mapKey,
	})

	jsonData, err := json.MarshalIndent(newCatalog, "", "  ")
	if err != nil {
		return fmt.Errorf("catalog json marshal error: %w", err)
	}
	if err := putObj(ctx, client, cfg.CatalogKey, jsonData, cfg); err != nil {
		return fmt.Errorf("put catalog object error: %w", err)
	}
	return nil
}

func listFiles(ctx context.Context, client *s3.Client, cfg S3ProviderConfig) {
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.BucketName),
		Prefix: aws.String("snapshots/"),
	})
	var snapshots []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			log.Println("Failed to list snapshots:", err)
			return
		}
		for _, obj := range page.Contents {
			if strings.HasSuffix(*obj.Key, "/catalog.json") {
				snapshotID := strings.TrimPrefix(strings.TrimSuffix(*obj.Key, "/catalog.json"), "snapshots/")
				snapshots = append(snapshots, snapshotID)
			}
		}
	}

	if len(snapshots) > 0 {
		fmt.Println("Available snapshots:")
		for _, snapshotID := range snapshots {
			fmt.Printf("- %s\n", snapshotID)
		}
	}

	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.BucketName),
		Key:    aws.String(cfg.CatalogKey),
	})
	if err != nil {
		log.Println("Global catalog not found:", err)
		return
	}
	defer out.Body.Close()

	var catalog []CatalogEntry
	if err := json.NewDecoder(out.Body).Decode(&catalog); err != nil {
		log.Println("Failed to parse global catalog:", err)
		return
	}

	fmt.Println("Files in global catalog:")
	for _, entry := range catalog {
		fmt.Printf("- %s (%d bytes)\n", entry.FileName, entry.FileSize)
	}
}

func listSnapshotFiles(ctx context.Context, client *s3.Client, snapshotID string, cfg S3ProviderConfig) {
	snapshotCatalogKey := fmt.Sprintf("snapshots/%s/catalog.json", snapshotID)
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.BucketName),
		Key:    aws.String(snapshotCatalogKey),
	})
	if err != nil {
		log.Printf("Snapshot catalog %s not found: %v", snapshotID, err)
		return
	}
	defer out.Body.Close()

	var snapshot SnapshotCatalog
	if err := json.NewDecoder(out.Body).Decode(&snapshot); err != nil {
		log.Printf("Failed to parse snapshot catalog %s: %v", snapshotID, err)
		return
	}

	fmt.Printf("Files in snapshot %s (created %s by %s):\n", snapshot.SnapshotID, snapshot.Timestamp.Format(time.RFC3339), snapshot.ComputerID)
	for _, entry := range snapshot.Files {
		fmt.Printf("- %s (%d bytes)\n", entry.FileName, entry.FileSize)
	}
}

func getFile(ctx context.Context, client *s3.Client, snapshotID, fileName, outDir string, cfg S3ProviderConfig) {
	var mapKey string
	if snapshotID != "" {
		mapKey = fmt.Sprintf("snapshots/%s/maps/%s.json", snapshotID, fileName)
	} else {
		mapKey = fmt.Sprintf("maps/%s.json", fileName)
	}

	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.BucketName),
		Key:    aws.String(mapKey),
	})
	if err != nil {
		log.Printf("File map not found for %s: %v", fileName, err)
		return
	}
	defer out.Body.Close()

	var fm FileMap
	if err := json.NewDecoder(out.Body).Decode(&fm); err != nil {
		log.Printf("Failed to decode file map for %s: %v", fileName, err)
		return
	}

	if err := os.MkdirAll(outDir, 0755); err != nil {
		log.Printf("Failed to create output directory %s: %v", outDir, err)
		return
	}
	outPath := filepath.Join(outDir, filepath.Base(fileName))
	f, err := os.Create(outPath)
	if err != nil {
		log.Printf("Failed to create output file %s: %v", outPath, err)
		return
	}
	defer f.Close()

	for _, blockHash := range fm.Blocks {
		blockKey := "blocks/" + blockHash
		blkObj, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(cfg.BucketName),
			Key:    aws.String(blockKey),
		})
		if err != nil {
			log.Printf("Failed to get block %s: %v", blockHash, err)
			return
		}
		_, err = io.Copy(f, blkObj.Body)
		blkObj.Body.Close()
		if err != nil {
			log.Printf("Error writing block %s: %v", blockHash, err)
			return
		}
	}

	fmt.Printf("File reconstructed to: %s\n", outPath)
}

func getFileMap(ctx context.Context, client *s3.Client, fileName string, cfg S3ProviderConfig) {
	mapKey := fmt.Sprintf("maps/%s.json", fileName)
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.BucketName),
		Key:    aws.String(mapKey),
	})
	if err != nil {
		log.Printf("File map not found for %s: %v", fileName, err)
		return
	}
	defer out.Body.Close()

	var fm FileMap
	if err := json.NewDecoder(out.Body).Decode(&fm); err != nil {
		log.Printf("Failed to decode file map for %s: %v", fileName, err)
		return
	}

	fmt.Printf("File Map for %s:\n", fileName)
	fmt.Printf("  File Name: %s\n", fm.FileName)
	fmt.Printf("  File Size: %d bytes\n", fm.FileSize)
	fmt.Printf("  Block Size: %d bytes\n", fm.BlockSize)
	fmt.Println("  Blocks:")
	for i, blockHash := range fm.Blocks {
		fmt.Printf("    %d: %s\n", i+1, blockHash)
	}
}

func createSnapshot(ctx context.Context, client *s3.Client, uploader *manager.Uploader, dir, snapshotID string, changedFiles []string, cfg S3ProviderConfig) {
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("Failed to get hostname: %v, using 'unknown'", err)
		hostname = "unknown"
	}
	snapshotCatalog := SnapshotCatalog{
		SnapshotID: snapshotID,
		Timestamp:  time.Now(),
		ComputerID: hostname,
		Files:      []CatalogEntry{},
	}

	var filesToProcess []string
	if len(changedFiles) > 0 {
		for _, relPath := range changedFiles {
			fullPath := filepath.Join(dir, relPath)
			if info, err := os.Stat(fullPath); err == nil && !info.IsDir() {
				filesToProcess = append(filesToProcess, fullPath)
			} else {
				log.Printf("Skipping invalid or directory file %s: %v", fullPath, err)
			}
		}
	} else {
		err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				filesToProcess = append(filesToProcess, path)
			}
			return nil
		})
		if err != nil {
			log.Printf("Failed to list directory %s: %v", dir, err)
			return
		}
	}

	for _, fullPath := range filesToProcess {
		relPath, err := filepath.Rel(dir, fullPath)
		if err != nil {
			relPath = filepath.Base(fullPath)
		}
		fmt.Println("Processing for snapshot:", fullPath)
		f, err := os.Open(fullPath)
		if err != nil {
			log.Printf("Open error for %s: %v", fullPath, err)
			continue
		}
		defer f.Close()

		fileInfo, err := f.Stat()
		if err != nil {
			log.Printf("Stat error for %s: %v", fullPath, err)
			continue
		}

		lockKey := fmt.Sprintf("locks/snapshots/%s/%s.lock", snapshotID, relPath)
		if err := acquireLock(ctx, client, lockKey, hostname, cfg); err != nil {
			log.Printf("Skipping file %s due to lock failure: %v", relPath, err)
			continue
		}
		defer releaseLock(ctx, client, lockKey, cfg)

		var blocks []string
		buf := make([]byte, cfg.BlockSize)
		for {
			n, err := f.Read(buf)
			if n > 0 {
				blockHash := hashBlock(buf[:n])
				if err := uploadBlock(ctx, client, blockHash, buf[:n], cfg); err != nil {
					log.Printf("Upload block error for %s: %v", fullPath, err)
					continue
				}
				blocks = append(blocks, blockHash)
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Printf("Read error for %s: %v", fullPath, err)
				continue
			}
		}

		fm := FileMap{
			FileName:  relPath,
			FileSize:  fileInfo.Size(),
			BlockSize: cfg.BlockSize,
			Blocks:    blocks,
		}

		mapKey := fmt.Sprintf("snapshots/%s/maps/%s.json", snapshotID, relPath)
		mapJSON, err := json.Marshal(fm)
		if err != nil {
			log.Printf("JSON marshal error for %s: %v", fullPath, err)
			continue
		}
		if err := putObj(ctx, client, mapKey, mapJSON, cfg); err != nil {
			log.Printf("Put map object error for %s: %v", fullPath, err)
			continue
		}

		snapshotCatalog.Files = append(snapshotCatalog.Files, CatalogEntry{
			FileName: relPath,
			FileSize: fileInfo.Size(),
			MapKey:   mapKey,
		})
	}

	snapshotCatalogKey := fmt.Sprintf("snapshots/%s/catalog.json", snapshotID)
	jsonData, err := json.MarshalIndent(snapshotCatalog, "", "  ")
	if err != nil {
		log.Printf("Snapshot catalog JSON marshal error: %v", err)
		return
	}
	if err := putObj(ctx, client, snapshotCatalogKey, jsonData, cfg); err != nil {
		log.Printf("Put snapshot catalog error: %v", err)
		return
	}

	fmt.Printf("Snapshot %s created with %d files\n", snapshotID, len(snapshotCatalog.Files))
}

func deleteSnapshot(ctx context.Context, client *s3.Client, snapshotID string, cfg S3ProviderConfig) {
	snapshotCatalogKey := fmt.Sprintf("snapshots/%s/catalog.json", snapshotID)
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.BucketName),
		Key:    aws.String(snapshotCatalogKey),
	})
	if err != nil {
		log.Printf("Snapshot %s not found: %v", snapshotID, err)
		return
	}
	defer out.Body.Close()

	var snapshot SnapshotCatalog
	if err := json.NewDecoder(out.Body).Decode(&snapshot); err != nil {
		log.Printf("Failed to parse snapshot catalog %s: %v", snapshotID, err)
		return
	}

	// Delete file maps
	for _, entry := range snapshot.Files {
		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(cfg.BucketName),
			Key:    aws.String(entry.MapKey),
		})
		if err != nil {
			log.Printf("Failed to delete map %s: %v", entry.MapKey, err)
		}
	}

	// Delete catalog
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(cfg.BucketName),
		Key:    aws.String(snapshotCatalogKey),
	})
	if err != nil {
		log.Printf("Failed to delete snapshot catalog %s: %v", snapshotCatalogKey, err)
		return
	}

	// Clean up locks
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.BucketName),
		Prefix: aws.String(fmt.Sprintf("locks/snapshots/%s/", snapshotID)),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			log.Printf("Failed to list locks for snapshot %s: %v", snapshotID, err)
			continue
		}
		for _, obj := range page.Contents {
			_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(cfg.BucketName),
				Key:    obj.Key,
			})
			if err != nil {
				log.Printf("Failed to delete lock %s: %v", *obj.Key, err)
			}
		}
	}

	fmt.Printf("Snapshot %s deleted\n", snapshotID)
}

func cleanupBlocks(ctx context.Context, client *s3.Client, owner string, cfg S3ProviderConfig) {
	// Acquire a global cleanup lock to prevent concurrent snapshot creation
	cleanupLockKey := "locks/global/cleanup.lock"
	if err := acquireLock(ctx, client, cleanupLockKey, owner, cfg); err != nil {
		log.Fatalf("Failed to acquire cleanup lock: %v", err)
	}
	defer releaseLock(ctx, client, cleanupLockKey, cfg)

	// Collect all referenced block hashes
	referencedBlocks := make(map[string]struct{})

	// 1. Scan snapshot catalogs
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.BucketName),
		Prefix: aws.String("snapshots/"),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			log.Printf("Failed to list snapshots: %v", err)
			continue
		}
		for _, obj := range page.Contents {
			if strings.HasSuffix(*obj.Key, "/catalog.json") {
				snapshotCatalogKey := *obj.Key
				out, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(cfg.BucketName),
					Key:    aws.String(snapshotCatalogKey),
				})
				if err != nil {
					log.Printf("Failed to get snapshot catalog %s: %v", snapshotCatalogKey, err)
					continue
				}
				var snapshot SnapshotCatalog
				if err := json.NewDecoder(out.Body).Decode(&snapshot); err != nil {
					log.Printf("Failed to parse snapshot catalog %s: %v", snapshotCatalogKey, err)
					out.Body.Close()
					continue
				}
				out.Body.Close()

				// Read file maps
				for _, entry := range snapshot.Files {
					mapOut, err := client.GetObject(ctx, &s3.GetObjectInput{
						Bucket: aws.String(cfg.BucketName),
						Key:    aws.String(entry.MapKey),
					})
					if err != nil {
						log.Printf("Failed to get file map %s: %v", entry.MapKey, err)
						continue
					}
					var fm FileMap
					if err := json.NewDecoder(mapOut.Body).Decode(&fm); err != nil {
						log.Printf("Failed to parse file map %s: %v", entry.MapKey, err)
						mapOut.Body.Close()
						continue
					}
					mapOut.Body.Close()
					for _, blockHash := range fm.Blocks {
						referencedBlocks[blockHash] = struct{}{}
					}
				}
			}
		}
	}

	// 2. Scan global catalog
	globalCatalogOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.BucketName),
		Key:    aws.String(cfg.CatalogKey),
	})
	if err == nil {
		var catalog []CatalogEntry
		if err := json.NewDecoder(globalCatalogOut.Body).Decode(&catalog); err != nil {
			log.Printf("Failed to parse global catalog: %v", err)
		} else {
			for _, entry := range catalog {
				mapOut, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(cfg.BucketName),
					Key:    aws.String(entry.MapKey),
				})
				if err != nil {
					log.Printf("Failed to get file map %s: %v", entry.MapKey, err)
					continue
				}
				var fm FileMap
				if err := json.NewDecoder(mapOut.Body).Decode(&fm); err != nil {
					log.Printf("Failed to parse file map %s: %v", entry.MapKey, err)
					mapOut.Body.Close()
					continue
				}
				mapOut.Body.Close()
				for _, blockHash := range fm.Blocks {
					referencedBlocks[blockHash] = struct{}{}
				}
			}
		}
		globalCatalogOut.Body.Close()
	} else {
		log.Printf("Global catalog not found: %v", err)
	}

	// 3. List all blocks and delete unreferenced ones
	blockPaginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.BucketName),
		Prefix: aws.String("blocks/"),
	})
	var deletedBlocks int
	for blockPaginator.HasMorePages() {
		page, err := blockPaginator.NextPage(ctx)
		if err != nil {
			log.Printf("Failed to list blocks: %v", err)
			continue
		}
		for _, obj := range page.Contents {
			blockHash := strings.TrimPrefix(*obj.Key, "blocks/")
			if _, referenced := referencedBlocks[blockHash]; !referenced {
				_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(cfg.BucketName),
					Key:    obj.Key,
				})
				if err != nil {
					log.Printf("Failed to delete block %s: %v", blockHash, err)
					continue
				}
				fmt.Printf("Deleted unreferenced block: %s\n", blockHash)
				deletedBlocks++
			}
		}
	}

	fmt.Printf("Block cleanup completed: %d blocks deleted\n", deletedBlocks)
}

func deleteFile(ctx context.Context, client *s3.Client, owner, fileName string, cfg S3ProviderConfig) {
	// Check if file is in any snapshot
	snapshotFiles := make(map[string]struct{})
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.BucketName),
		Prefix: aws.String("snapshots/"),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			log.Printf("Failed to list snapshots: %v", err)
			continue
		}
		for _, obj := range page.Contents {
			if strings.HasSuffix(*obj.Key, "/catalog.json") {
				snapshotCatalogKey := *obj.Key
				out, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(cfg.BucketName),
					Key:    aws.String(snapshotCatalogKey),
				})
				if err != nil {
					log.Printf("Failed to get snapshot catalog %s: %v", snapshotCatalogKey, err)
					continue
				}
				var snapshot SnapshotCatalog
				if err := json.NewDecoder(out.Body).Decode(&snapshot); err != nil {
					log.Printf("Failed to parse snapshot catalog %s: %v", snapshotCatalogKey, err)
					out.Body.Close()
					continue
				}
				out.Body.Close()
				for _, entry := range snapshot.Files {
					snapshotFiles[entry.FileName] = struct{}{}
				}
			}
		}
	}

	// if _, inSnapshot := snapshotFiles[fileName]; inSnapshot {
	// 	fmt.Printf("Cannot delete %s: referenced in a snapshot\n", fileName)
	// 	return
	// }

	// Acquire global catalog lock
	catalogLockKey := "locks/global/catalog.lock"
	if err := acquireLock(ctx, client, catalogLockKey, owner, cfg); err != nil {
		log.Fatalf("Failed to acquire catalog lock: %v", err)
	}
	defer releaseLock(ctx, client, catalogLockKey, cfg)

	// Read global catalog
	var catalog []CatalogEntry
	var targetEntry *CatalogEntry
	globalCatalogOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.BucketName),
		Key:    aws.String(cfg.CatalogKey),
	})
	if err != nil {
		log.Printf("Global catalog not found: %v", err)
		fmt.Printf("File %s not found in global catalog\n", fileName)
		return
	}
	defer globalCatalogOut.Body.Close()
	if err := json.NewDecoder(globalCatalogOut.Body).Decode(&catalog); err != nil {
		log.Printf("Failed to parse global catalog: %v", err)
		return
	}

	// Find the target file
	for _, entry := range catalog {
		if entry.FileName == fileName {
			targetEntry = &entry
			break
		}
	}
	if targetEntry == nil {
		fmt.Printf("File %s not found in global catalog\n", fileName)
		return
	}

	// Acquire lock for file map deletion
	lockKey := fmt.Sprintf("locks/global/%s.lock", fileName)
	if err := acquireLock(ctx, client, lockKey, owner, cfg); err != nil {
		log.Printf("Failed to acquire lock for %s: %v", fileName, err)
		fmt.Printf("Cannot delete %s: failed to acquire lock\n", fileName)
		return
	}
	defer releaseLock(ctx, client, lockKey, cfg)

	// Delete file map
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(cfg.BucketName),
		Key:    aws.String(targetEntry.MapKey),
	})
	if err != nil {
		log.Printf("Failed to delete file map %s: %v", targetEntry.MapKey, err)
		fmt.Printf("Failed to delete file map for %s\n", fileName)
		return
	}

	// Update global catalog
	newCatalog := make([]CatalogEntry, 0, len(catalog)-1)
	for _, entry := range catalog {
		if entry.FileName != fileName {
			newCatalog = append(newCatalog, entry)
		}
	}
	jsonData, err := json.MarshalIndent(newCatalog, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal new catalog: %v", err)
		fmt.Printf("Failed to update global catalog for %s\n", fileName)
		return
	}
	if err := putObj(ctx, client, cfg.CatalogKey, jsonData, cfg); err != nil {
		log.Printf("Failed to update global catalog: %v", err)
		fmt.Printf("Failed to update global catalog for %s\n", fileName)
		return
	}

	fmt.Printf("Deleted file: %s\n", fileName)

	// Clean up unreferenced blocks
	cleanupLockKey := "locks/global/cleanup.lock"
	if err := acquireLock(ctx, client, cleanupLockKey, owner, cfg); err != nil {
		log.Printf("Failed to acquire cleanup lock: %v, skipping block cleanup", err)
		return
	}
	defer releaseLock(ctx, client, cleanupLockKey, cfg)

	referencedBlocks := make(map[string]struct{})
	// Scan snapshot catalogs for blocks
	paginator = s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.BucketName),
		Prefix: aws.String("snapshots/"),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			log.Printf("Failed to list snapshots for block cleanup: %v", err)
			continue
		}
		for _, obj := range page.Contents {
			if strings.HasSuffix(*obj.Key, "/catalog.json") {
				snapshotCatalogKey := *obj.Key
				out, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(cfg.BucketName),
					Key:    aws.String(snapshotCatalogKey),
				})
				if err != nil {
					log.Printf("Failed to get snapshot catalog %s: %v", snapshotCatalogKey, err)
					continue
				}
				var snapshot SnapshotCatalog
				if err := json.NewDecoder(out.Body).Decode(&snapshot); err != nil {
					log.Printf("Failed to parse snapshot catalog %s: %v", snapshotCatalogKey, err)
					out.Body.Close()
					continue
				}
				out.Body.Close()
				for _, entry := range snapshot.Files {
					mapOut, err := client.GetObject(ctx, &s3.GetObjectInput{
						Bucket: aws.String(cfg.BucketName),
						Key:    aws.String(entry.MapKey),
					})
					if err != nil {
						log.Printf("Failed to get file map %s: %v", entry.MapKey, err)
						continue
					}
					var fm FileMap
					if err := json.NewDecoder(mapOut.Body).Decode(&fm); err != nil {
						log.Printf("Failed to parse file map %s: %v", entry.MapKey, err)
						mapOut.Body.Close()
						continue
					}
					mapOut.Body.Close()
					for _, blockHash := range fm.Blocks {
						referencedBlocks[blockHash] = struct{}{}
					}
				}
			}
		}
	}

	// Scan updated global catalog for blocks
	for _, entry := range newCatalog {
		mapOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(cfg.BucketName),
			Key:    aws.String(entry.MapKey),
		})
		if err != nil {
			log.Printf("Failed to get file map %s: %v", entry.MapKey, err)
			continue
		}
		var fm FileMap
		if err := json.NewDecoder(mapOut.Body).Decode(&fm); err != nil {
			log.Printf("Failed to parse file map %s: %v", entry.MapKey, err)
			mapOut.Body.Close()
			continue
		}
		mapOut.Body.Close()
		for _, blockHash := range fm.Blocks {
			referencedBlocks[blockHash] = struct{}{}
		}
	}

	// Delete unreferenced blocks
	blockPaginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(cfg.BucketName),
		Prefix: aws.String("blocks/"),
	})
	var deletedBlocks int
	for blockPaginator.HasMorePages() {
		page, err := blockPaginator.NextPage(ctx)
		if err != nil {
			log.Printf("Failed to list blocks: %v", err)
			continue
		}
		for _, obj := range page.Contents {
			blockHash := strings.TrimPrefix(*obj.Key, "blocks/")
			if _, referenced := referencedBlocks[blockHash]; !referenced {
				_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(cfg.BucketName),
					Key:    obj.Key,
				})
				if err != nil {
					log.Printf("Failed to delete block %s: %v", blockHash, err)
					continue
				}
				fmt.Printf("Deleted unreferenced block: %s\n", blockHash)
				deletedBlocks++
			}
		}
	}

	fmt.Printf("Block cleanup completed: %d blocks deleted\n", deletedBlocks)
}
