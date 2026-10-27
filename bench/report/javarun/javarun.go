// Package javarun runs the Java JMH suite through its wrapper script.
package javarun

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"syscall"
)

// Tools are the executables the wrapper script needs.
var Tools = []string{"java", "mvn"}

// Available reports the first missing tool, or "" when Java can run.
func Available() (missing string) {
	for _, t := range Tools {
		if _, err := exec.LookPath(t); err != nil {
			return t
		}
	}

	return ""
}

// Run executes script, which writes the report JSON to out, passing jmhArgs through to JMH.
func Run(ctx context.Context, script, out string, jmhArgs []string, progress io.Writer) error {
	abs, err := filepath.Abs(out)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, script, append([]string{"-o", abs}, jmhArgs...)...) //nolint:gosec // Script path is a fixed repo file.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM) }
	cmd.Stdout = progress
	cmd.Stderr = progress
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("run %s: %w", script, err)
	}

	return nil
}
