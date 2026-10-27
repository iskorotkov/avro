package dataset

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

// Fetch downloads src into dir unless a file with the pinned checksum is already there.
func Fetch(ctx context.Context, client *http.Client, dir string, src Source) error {
	path := filepath.Join(dir, src.File)
	if sum, err := fileSHA256(path); err == nil && sum == src.SHA256 {
		return nil
	}

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}

	tmp := path + ".part"
	if err := download(ctx, client, src.URL, tmp); err != nil {
		return err
	}

	sum, err := fileSHA256(tmp)
	if err != nil {
		return err
	}
	if sum != src.SHA256 {
		_ = os.Remove(tmp)
		return fmt.Errorf("%s: sha256 %s, want %s", src.URL, sum, src.SHA256)
	}

	return os.Rename(tmp, path)
}

func download(ctx context.Context, client *http.Client, url, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, resp.Status)
	}

	f, err := os.Create(filepath.Clean(path))
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		return err
	}

	return f.Close()
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
