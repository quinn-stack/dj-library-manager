import subprocess
from PySide6.QtCore import QThread, Signal


class CommandRunner(QThread):
    output_signal = Signal(str)
    finished_signal = Signal(int)

    def __init__(self, command, working_dir=None):
        super().__init__()
        self.command = command
        self.working_dir = working_dir
        self.process = None

    def run(self):
        try:
            self.process = subprocess.Popen(
                self.command,
                cwd=self.working_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                shell=True
            )

            for line in self.process.stdout:
                self.output_signal.emit(line.rstrip())

            self.process.wait()
            self.finished_signal.emit(self.process.returncode)

        except Exception as e:
            self.output_signal.emit(f"ERROR: {str(e)}")
            self.finished_signal.emit(-1)

    def stop(self):
        if self.process:
            self.process.terminate()


class TaskRunner(QThread):
    """Run a Python callable inside a QThread and emit simple signals.

    Use this for CPU- or I/O-bound Python tasks that shouldn't block the UI.
    """
    output_signal = Signal(str)
    finished_signal = Signal(object)

    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            result = self.func(*self.args, **self.kwargs)
            self.finished_signal.emit(result)
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            try:
                self.output_signal.emit(f"ERROR: {e}\n{tb}")
            except Exception:
                pass
            # Emit a dict with error info rather than bare None — bare None is
            # indistinguishable from a successful empty result in most callers,
            # causing silent "no results" behaviour instead of a visible error.
            self.finished_signal.emit({"__task_error__": str(e), "__traceback__": tb})
