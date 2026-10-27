package run

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

// Execute runs all rounds and returns the path of the dated raw results file.
// Progress goes to progress; latest.txt in ResultsDir is refreshed.
func Execute(ctx context.Context, o Options, progress io.Writer, now time.Time) (string, error) {
	rev, err := GitRevision(ctx, o.Dir)
	if err != nil {
		return "", err
	}
	goVersion, err := GoVersion(ctx, o.Dir)
	if err != nil {
		return "", err
	}

	tmp, err := os.MkdirTemp("", "report-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tmp) //nolint:errcheck // Best-effort cleanup.

	binary := filepath.Join(tmp, "suite.test")
	logf(progress, "building %s\n", o.Package)
	if err := o.build(ctx, binary, progress); err != nil {
		return "", err
	}

	if o.CPUProfileDir != "" {
		if err := os.MkdirAll(abs(o.Dir, o.CPUProfileDir), 0o750); err != nil {
			return "", err
		}
	}

	resultsDir := abs(o.Dir, o.ResultsDir)
	if err := os.MkdirAll(resultsDir, 0o750); err != nil {
		return "", err
	}
	path := filepath.Join(resultsDir, fmt.Sprintf("%s-%s.txt", now.Format(time.DateOnly), rev.Short))
	if err := o.record(ctx, binary, path, Header{Commit: rev.Full, Date: now, Go: goVersion, GOMAXPROCS: o.Procs, Flags: o.FlagsLine()}, progress); err != nil {
		return "", err
	}

	return path, copyFile(path, filepath.Join(resultsDir, "latest.txt"))
}

// record streams the header and every round into path so a failed round keeps earlier ones.
func (o Options) record(ctx context.Context, binary, path string, header Header, progress io.Writer) error {
	f, err := os.Create(filepath.Clean(path))
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if _, err := header.WriteTo(f); err != nil {
		return err
	}
	for round := range o.Count {
		logf(progress, "round %d/%d\n", round+1, o.Count)
		if err := o.round(ctx, binary, round, f, progress); err != nil {
			return fmt.Errorf("round %d: %w", round+1, err)
		}
	}

	return f.Close()
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(filepath.Clean(src))
	if err != nil {
		return err
	}

	return os.WriteFile(dst, data, 0o600) //nolint:gosec // Path is built from the user's flags.
}

func (o Options) build(ctx context.Context, binary string, progress io.Writer) error {
	cmd := exec.CommandContext(ctx, "go", "test", "-c", "-o", binary, o.Package) //nolint:gosec // Package comes from the tool itself.
	cmd.Dir = o.Dir
	cmd.Stdout, cmd.Stderr = progress, progress
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build suite: %w", err)
	}
	return nil
}

func (o Options) round(ctx context.Context, binary string, round int, out, progress io.Writer) error {
	cmd := exec.CommandContext(ctx, binary, o.testArgs(round)...) //nolint:gosec // Binary was just built by this tool.
	cmd.Dir = abs(o.Dir, o.Package)
	cmd.Env = append(os.Environ(), "GOMAXPROCS="+strconv.Itoa(o.Procs))
	cmd.Stdout, cmd.Stderr = io.MultiWriter(out, progress), progress
	return cmd.Run()
}

func (o Options) testArgs(round int) []string {
	args := []string{
		"-test.run=^$",
		"-test.bench=" + o.BenchRegex(),
		"-test.benchmem",
		"-test.count=1",
		"-test.benchtime=" + o.Benchtime,
		"-test.cpu=" + strconv.Itoa(o.Procs),
		"-suite.rotate=" + strconv.Itoa(round),
		"-suite.dataset=^(" + orAny(o.Dataset) + ")$",
		"-suite.format=^(" + orAny(o.Format) + ")$",
	}
	if o.DataDir != "" {
		args = append(args, "-suite.data="+abs(o.Dir, o.DataDir))
	}
	if o.CPUProfileDir != "" {
		args = append(args, "-test.cpuprofile="+filepath.Join(abs(o.Dir, o.CPUProfileDir), fmt.Sprintf("round-%d.pprof", round)))
	}
	return args
}

func abs(dir, path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(dir, path)
}

func logf(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}
