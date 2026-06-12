package manager

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// Upload uploads a file to S3 using multipart upload, respecting context cancellation.
func (u *Uploader) Upload(ctx context.Context, input *UploadInput) error {
	// ... (existing setup code)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// ... (multipart initiation)

	var wg sync.WaitGroup
	sem := make(chan struct{}, u.Concurrency)

	for _, part := range parts {
		select {
		case <-ctx.Done():
			wg.Wait()
			if !u.LeavePartsOnError {
				u.abortMultipartUpload(context.Background(), input)
			}
			return ctx.Err()
		case sem <- struct{}{}:
			wg.Add(1)
			go func(p Part) {
				defer wg.Done()
				defer func() { <-sem }()
				// Pass the context to the upload part call
				u.uploadPart(ctx, p)
			}(part)
		}
	}

	wg.Wait()
	return nil
}

func (u *Uploader) uploadPart(ctx context.Context, p Part) error {
	// Ensure the underlying S3 client call uses the provided context
	_, err := u.S3.UploadPart(ctx, &UploadPartInput{...})
	return err
}