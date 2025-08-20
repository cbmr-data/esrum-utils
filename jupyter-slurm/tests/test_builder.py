from __future__ import annotations

import getpass
from typing import Literal

import pytest

from jupyter_slurm import sbatch_script, slurm_options, srun_command

DEFAULT_EMAIL = f"{getpass.getuser()}@ku.dk"

########################################################################################


def test_sbatch_script__defaults() -> None:
    assert sbatch_script(["ls"]) == [
        "#!/bin/bash\n",
        "ls\n",
    ]


def test_sbatch_script__strict() -> None:
    assert sbatch_script(["ls"], strict=True) == [
        "#!/bin/bash\n",
        "set -euo pipefail\n",
        'trap \'s=$?; echo >&2 "$0: Error on line "$LINENO": $BASH_COMMAND"; exit $s\''
        " ERR\n",
        "ls\n",
    ]


def test_sbatch_script__mail_user() -> None:
    assert sbatch_script(["ls"], mail_user=True) == [
        "#!/bin/bash\n",
        f"#SBATCH --mail-user={DEFAULT_EMAIL}\n",
        "#SBATCH --mail-type=END,FAIL\n",
        "ls\n",
    ]

    assert sbatch_script(["ls"], mail_user="foo@bar.com") == [
        "#!/bin/bash\n",
        "#SBATCH --mail-user=foo@bar.com\n",
        "#SBATCH --mail-type=END,FAIL\n",
        "ls\n",
    ]


def test_sbatch_script__cpus() -> None:
    assert sbatch_script(["ls"], cpus=13) == [
        "#!/bin/bash\n",
        "#SBATCH --cpus-per-task=13\n",
        "ls\n",
    ]


def test_sbatch_script__memory() -> None:
    assert sbatch_script(["ls"], modules=("python/3.12.2", "gcc/8.5.0")) == [
        "#!/bin/bash\n",
        "module load python/3.12.2\n",
        "module load gcc/8.5.0\n",
        "ls\n",
    ]


def test_sbatch_script__modules() -> None:
    assert sbatch_script(["ls"], memory="123M") == [
        "#!/bin/bash\n",
        "#SBATCH --mem=123M\n",
        "ls\n",
    ]


########################################################################################
# srun_command


def test_srun_command__defaults() -> None:
    assert srun_command() == ["/usr/bin/srun"]


def test_srun_command__cpus() -> None:
    assert srun_command(cpus=42) == [
        "/usr/bin/srun",
        "--cpus-per-task=42",
    ]


def test_srun_command__memory() -> None:
    assert srun_command(memory="420G") == [
        "/usr/bin/srun",
        "--mem=420G",
    ]


########################################################################################
# slurm_options -- argument validation


def test_slurm_options__cpus() -> None:
    assert slurm_options(cpus=1) == []
    assert slurm_options(cpus=128) == ["--cpus-per-task=128"]

    with pytest.raises(ValueError, match="cpus must be in the range"):
        assert slurm_options(cpus=0)
    with pytest.raises(ValueError, match="cpus must be in the range"):
        assert slurm_options(cpus=129)


def test_slurm_options__memory() -> None:
    assert slurm_options(memory=17) == ["--mem=17M"]

    assert slurm_options(memory="1M") == ["--mem=1M"]
    assert slurm_options(memory="1023M") == ["--mem=1023M"]
    assert slurm_options(memory="1024M") == ["--mem=1024M"]
    assert slurm_options(memory="1025M") == ["--mem=1025M"]

    assert slurm_options(memory="1G") == ["--mem=1G"]
    assert slurm_options(memory="1023G") == ["--mem=1023G"]
    assert slurm_options(memory="1024G") == ["--mem=1024G"]
    assert slurm_options(memory="1025G") == ["--mem=1025G"]

    assert slurm_options(memory="1T") == ["--mem=1T"]
    assert slurm_options(memory="2T") == ["--mem=2T", "--partition=gpuqueue"]

    assert slurm_options(memory="1993G") == ["--mem=1993G"]
    assert slurm_options(memory="1994G") == ["--mem=1994G", "--partition=gpuqueue"]

    with pytest.raises(ValueError, match="`memory` greater than high-memory node"):
        slurm_options(memory="4T")

    assert slurm_options(memory=" 1T\n") == ["--mem=1T"]


@pytest.mark.parametrize("value", [-12, "-4G", "5X"])
def test_slurm_options__invalid_memory_values(value: str | int) -> None:
    with pytest.raises(ValueError, match="invalid `memory` value"):
        slurm_options(memory=value)


@pytest.mark.parametrize("value", [0, "0", "0M"])
def test_slurm_options__zero_memory_values(value: str | int) -> None:
    with pytest.raises(ValueError, match="non-positive `memory` value"):
        slurm_options(memory=value)


def test_slurm_options__gpus() -> None:
    assert slurm_options(gpus=0) == []
    assert slurm_options(gpus=1) == ["--gres=gpu:1", "--partition=gpuqueue"]
    assert slurm_options(gpus=2) == ["--gres=gpu:2", "--partition=gpuqueue"]


@pytest.mark.parametrize("value", ["a100", "h100", "A100", "H100"])
def test_slurm_options__gpus_with_gpu_type(
    value: Literal["a100", "h100", "A100", "H100"],
) -> None:
    assert slurm_options(gpus=0, gpu_type=value) == []
    assert slurm_options(gpus=1, gpu_type=value) == [
        f"--gres=gpu:{value.lower()}:1",
        "--partition=gpuqueue",
    ]
    assert slurm_options(gpus=2, gpu_type=value) == [
        f"--gres=gpu:{value.lower()}:2",
        "--partition=gpuqueue",
    ]


@pytest.mark.parametrize("value", [-1, 3])
def test_slurm_options__invaild_gpu_values(value: int) -> None:
    with pytest.raises(ValueError, match="GPUs must be a 0, 1, or 2, not"):
        slurm_options(gpus=value)


def test_slurm_options__invaild_gpu_type() -> None:
    with pytest.raises(ValueError, match="unknown GPU type"):
        slurm_options(gpu_type="rtx3070")  # pyright: ignore[reportArgumentType]


def test_slurm_options__job_name() -> None:
    assert slurm_options(job_name="my job") == ["--job-name=my job"]


def test_slurm_options__job_array() -> None:
    assert slurm_options(array_params="1-10") == ["--array=1-10"]
