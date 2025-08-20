import unittest.mock

import pytest
from jupyter_slurm import SlurmError, SrunResult, srun


def test_srun_on_non_head_node() -> None:
    with (
        unittest.mock.patch("jupyter_slurm.IS_HEAD_NODE", new=False),
        unittest.mock.patch("jupyter_slurm.SRUN_CMD", new=("true",)),
        pytest.raises(SlurmError, match="`srun` can only be called on the head"),
    ):
        srun(["ls"])


def test_srun_on_fake_head_node() -> None:
    with (
        unittest.mock.patch("jupyter_slurm.IS_HEAD_NODE", new=True),
        unittest.mock.patch("jupyter_slurm.SRUN_CMD", new=("true",)),
    ):
        srun(["ls"])


def test_srun_capture() -> None:
    with (
        unittest.mock.patch("jupyter_slurm.IS_HEAD_NODE", new=True),
        # `nice` is used as safe "fake srun" since it does nothing to the command/output
        unittest.mock.patch("jupyter_slurm.SRUN_CMD", new=("nice",)),
    ):
        assert srun(["echo", "my-test"], capture=True) == SrunResult(
            returncode=0,
            stdout="my-test\n",
            stderr="",
        )

        assert srun(["echo", "my-test"], capture=True, text=False) == SrunResult(
            returncode=0,
            stdout=b"my-test\n",
            stderr=b"",
        )

        assert srun("echo my-test > /dev/stderr", capture=True) == SrunResult(
            returncode=0,
            stdout="",
            stderr="my-test\n",
        )


def test_srun_capture_stderr() -> None:
    with (
        unittest.mock.patch("jupyter_slurm.IS_HEAD_NODE", new=True),
        unittest.mock.patch(
            "jupyter_slurm.SRUN_CMD",
            new=("bash", "-c", "echo test failure >&2; exit 13"),
        ),
    ):
        assert srun(["my-test"], capture=True) == SrunResult(
            returncode=13,
            stdout="",
            stderr="test failure\n",
        )


def test_srun_return_code() -> None:
    with (
        unittest.mock.patch("jupyter_slurm.IS_HEAD_NODE", new=True),
        unittest.mock.patch("jupyter_slurm.SRUN_CMD", new=("false",)),
    ):
        assert srun(["my-test"]) == SrunResult(returncode=1, stdout=None, stderr=None)
