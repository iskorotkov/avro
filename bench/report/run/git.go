package run

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// Revision identifies the checked-out source.
type Revision struct {
	// Full is the commit hash with a -dirty suffix for a modified tree.
	Full string
	// Short is the abbreviated hash with a -dirty suffix for a modified tree.
	Short string
}

// GitRevision reads HEAD and the worktree state of the repository containing dir.
func GitRevision(ctx context.Context, dir string) (Revision, error) {
	full, err := output(ctx, dir, "git", "rev-parse", "HEAD")
	if err != nil {
		return Revision{}, err
	}
	short, err := output(ctx, dir, "git", "rev-parse", "--short", "HEAD")
	if err != nil {
		return Revision{}, err
	}
	status, err := output(ctx, dir, "git", "status", "--porcelain")
	if err != nil {
		return Revision{}, err
	}
	if status != "" {
		full += "-dirty"
		short += "-dirty"
	}
	return Revision{Full: full, Short: short}, nil
}

// GoVersion returns the toolchain version such as go1.26.0.
func GoVersion(ctx context.Context, dir string) (string, error) {
	out, err := output(ctx, dir, "go", "version")
	if err != nil {
		return "", err
	}
	fields := strings.Fields(out)
	if len(fields) < 3 {
		return "", fmt.Errorf("unexpected go version output %q", out)
	}
	return fields[2], nil
}

func output(ctx context.Context, dir, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...) //nolint:gosec // Callers pass fixed git and go commands.
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(out)), nil
}
