import os
import sys
from contextlib import contextmanager
from typing import Any, Generator

if os.name == "nt":
    import msvcrt

    def key_pressed() -> str | None:
        """Return key if pressed (non-blocking), else None. Windows implementation."""
        if msvcrt.kbhit():  # type: ignore # This definitely exists under windows
            return msvcrt.getwch()  # type: ignore # This definitely exists under windows
        return None

    def setup() -> None:
        """No setup needed for Windows."""
        pass

    def restore() -> None:
        """No restore needed for Windows."""
        pass

else:
    import select
    import termios
    import tty

    FD = sys.stdin.fileno()
    OLD_SETTINGS: Any | None = None

    def key_pressed() -> str | None:
        """Return key if pressed (non-blocking), else None. Unix implementation."""
        dr, _, _ = select.select([sys.stdin], [], [], 0)
        if dr:
            return sys.stdin.read(1)
        return None

    def setup() -> None:
        """Set terminal to raw mode (non-blocking, no line buffering)."""
        global OLD_SETTINGS
        OLD_SETTINGS = termios.tcgetattr(FD)
        tty.setcbreak(FD)

    def restore() -> None:
        """Restore terminal to original settings."""
        if OLD_SETTINGS:
            termios.tcsetattr(FD, termios.TCSADRAIN, OLD_SETTINGS)


@contextmanager
def activate_keypress() -> Generator[None, None, None]:
    setup()
    yield
    restore()
