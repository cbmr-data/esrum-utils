"""Python wrappers round the Slurm `sbatch` and `srun` commands.

This module is primarily intended for use in Jupyter notebooks, but can be used in
standalone scripts.
"""

from __future__ import annotations

import getpass
import itertools
import shlex
import socket
import subprocess
import tempfile
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Generic, Literal, Protocol, TypeVar, cast, overload

__all__ = [
    "SlurmError",
    "sbatch",
    "sbatch_script",
    "slurm_options",
    "srun",
    "srun_command",
]

# Minimum number of CPUs per job
MIN_CPUS = 1
# Maximum number of CPUs per job
MAX_CPUS = 128

MAX_LOW_MEMORY = 2041373 * 1024
MAX_HIGH_MEMORY = 4015755 * 1024

# Minimum number of GPUs per job
MIN_GPUS = 0
# Maximum number of GPUs per job
MAX_GPUS = 2
# The supported GPU types
GPU_TYPES = ("a100", "h100")

# Default `sbatch`/`srun` commands; can be overridden if necessary
SBATCH_CMD = ("/usr/bin/sbatch",)
SRUN_CMD = ("/usr/bin/srun",)
SHELL_CMD = "/bin/bash"

# Some operations can only be performed from the head node (sacct, srun)
HEAD_NODE = "esrumhead01fl.unicph.domain"
IS_HEAD_NODE = socket.gethostname() == HEAD_NODE


_T_out = TypeVar("_T_out", None, str, bytes)
_T_co = TypeVar("_T_co", covariant=True)


# Based on https://github.com/python/typing/issues/256#issuecomment-1442633430
class SequenceNotStr(Protocol[_T_co]):
    # We only need the sequence to be iterable
    def __iter__(self) -> Iterator[_T_co]: ...
    # This signature does not match `str.__contains__`, preventing use of raw strings
    def __contains__(self, value: object, /) -> bool: ...


class SlurmError(RuntimeError):
    """Errors relating to calling sbatch/srun."""


def slurm_options(
    *,
    cpus: int = 1,
    memory: int | str | None = None,
    gpus: int = 0,
    gpu_type: Literal["a100", "h100", "A100", "H100"] | None = None,
    job_name: str | None = None,
    extra_args: SequenceNotStr[str] = (),
    output_file: str | Path | None = None,
    array_params: str | None = None,
    mail_user: str | bool = False,
) -> list[str]:
    """Generate list of command-line options for Slurm.

    Args:
        cpus: The number of CPUs to reserve. Must be a number in the range 1 to 128.
            Defaults to 1.
        memory: The amount of memory to reserve. Must be a positive number (in MB) or a
            string ending with a unit (K, M, G, T). Defaults to ~16G per CPU.
        gpus: The number of CPUs to reserve, either 0, 1, or 2. Jobs that reserve CPUs
            will be run on the GPU queue. Defaults to 0.
        gpu_type: Preferred GPU type, if any, either 'a100' or 'h100'. Defaults to None.
        job_name: An optional string naming the current Slurm job.
        extra_args: A list of arguments passed directly to srun/sbatch. Multi-part
            arguments must therefore be split into multiple values:
            ["--foo", "bar"] and not ["--foo bar"]
        output_file: Optional name of log-file foom the job.
        array_params: Optional job-array parameters (see "--array").
        mail_user: Send an email to user on failures or completion of the job. May
            either be an email address, or `True` to send an email to `$USER@ku.dk`.

    Raises:
        ValueError: If any of the above arguments are invalid

    Returns:
        list[str]: A list of argument that can be passed to sbatch or srun.

    """
    args: list[str] = []
    if MIN_CPUS <= cpus <= MAX_CPUS:
        if cpus > 1:
            args.append(f"--cpus-per-task={cpus}")
    else:
        raise ValueError(f"cpus must be in the range {MIN_CPUS}-{MAX_CPUS}, not {cpus}")

    high_memory = False
    if memory is not None:
        memory, high_memory = _parse_memory(memory)

        args.append(f"--mem={_to_clean_str(memory)}")

    if gpu_type is not None and gpu_type.lower() not in GPU_TYPES:
        raise ValueError(f"unknown GPU type {gpu_type!r}")
    elif MIN_GPUS <= gpus <= MAX_GPUS:
        if gpus:
            gpu_request = gpus if gpu_type is None else f"{gpu_type.lower()}:{gpus}"
            args.append(f"--gres=gpu:{gpu_request}")
    else:
        raise ValueError(f"GPUs must be a 0, 1, or 2, not {gpus}")

    if high_memory or gpus:
        args.append("--partition=gpuqueue")

    if isinstance(mail_user, str) or mail_user:
        if not isinstance(mail_user, str):
            mail_user = f"{getpass.getuser()}@ku.dk"

        args.append(f"--mail-user={_to_clean_str(mail_user)}")
        args.append("--mail-type=END,FAIL")

    if job_name:
        args.append(f"--job-name={_to_clean_str(job_name)}")

    if array_params:
        args.append(f"--array={_to_clean_str(array_params)}")

    if output_file:
        args.append(f"--output={_to_clean_str(output_file)}")

    for line in extra_args:
        for it in line.splitlines():
            it = it.strip()
            if it:
                args.append(it)

    return args


def sbatch_script(
    commands: Sequence[str] | Sequence[Sequence[str]],
    *,
    cpus: int = 1,
    gpus: int = 0,
    gpu_type: Literal["a100", "h100", "A100", "H100"] | None = None,
    memory: int | str | None = None,
    job_name: str | None = None,
    modules: SequenceNotStr[str] = (),
    extra_args: SequenceNotStr[str] = (),
    output_file: str | Path | None = None,
    array_params: str | None = None,
    wait: bool = False,
    mail_user: str | bool = False,
    strict: bool = False,
) -> list[str]:
    """Generate sbatch script for running one or more commands.

    Args:
        commands: One or more commands to be run using sbatch. May be a list of strings,
            in which case the strings are assumed to be properly formatted commands and
            included as is, or a list of list of strings, in which case the each list of
            strings is assumed to represent a single command, and each argument is
            quoted/escaped to ensure that special characters are properly handled.
        cpus: The number of CPUs to reserve. Must be a number in the range 1 to 128.
            Defaults to 1.
        memory: The amount of memory to reserve. Must be a positive number (in MB) or a
            string ending with a unit (K, M, G, T). Defaults to ~16G per CPU.
        gpus: The number of CPUs to reserve, either 0, 1, or 2. Jobs that reserve CPUs
            will be run on the GPU queue. Defaults to 0.
        gpu_type: Preferred GPU type, if any, either 'a100' or 'h100'. Defaults to None.
        job_name: An optional string naming the current Slurm job.
        modules: A list of zero or more environment modules to load before running the
            commands specified above. Defaults to ().
        extra_args: A list of arguments passed directly to srun/sbatch. Multi-part
            arguments must therefore be split into multiple values:
            ["--foo", "bar"] and not ["--foo bar"]
        output_file: Optional name of log-file foom the job.
        array_params: Optional job-array parameters (see "--array").
        mail_user: Send an email to user on failures or completion of the job. May
            either be an email address, or `True` to send an email to `$USER@ku.dk`.
        wait: If true, wait for the job to complete before returning. Defaults to False.
        strict: If true, the script is configured to terminate on the first error.
            Defaults to true.

    Returns:
        list[str]: The sbatch script as a list of strings ending with newlines.

    """
    args = slurm_options(
        cpus=cpus,
        gpus=gpus,
        gpu_type=gpu_type,
        memory=memory,
        job_name=job_name,
        output_file=output_file,
        array_params=array_params,
        mail_user=mail_user,
    )

    script = [
        f"#!{SHELL_CMD}\n",
    ]

    for line in itertools.chain(args, extra_args):
        if not line.lstrip().startswith("#SBATCH "):
            line = f"#SBATCH {line}\n"

        script.append(line)

    if wait:
        script.append("#SBATCH --wait\n")

    if strict:
        # Exit on unset variables, pipe failure, and inherit ERR traps
        script.append("set -euo pipefail\n")
        # Print debug message and terminate script on non-zero return codes
        script.append(
            "trap 's=$?;"
            ' echo >&2 "$0: Error on line "$LINENO": $BASH_COMMAND";'
            " exit $s' ERR\n"
        )

    for module in modules:
        script.append(f"module load {module}\n")

    script.extend(_quote_commands(commands))

    return script


def sbatch(
    commands: Sequence[str] | Sequence[Sequence[str]],
    *,
    cpus: int = 1,
    gpus: int = 0,
    gpu_type: Literal["a100", "h100", "A100", "H100"] | None = None,
    memory: int | str | None = None,
    job_name: str | None = None,
    modules: SequenceNotStr[str] = (),
    extra_args: SequenceNotStr[str] = (),
    output_file: str | Path | None = None,
    array_params: str | None = None,
    wait: bool = False,
    mail_user: str | bool = False,
    strict: bool = True,
) -> int:
    """Submit an sbatch script for running one or more commands.

    Args:
        commands: One or more commands to be run using sbatch. May be a list of strings,
            in which case the strings are assumed to be properly formatted commands and
            included as is, or a list of list of strings, in which case the each list of
            strings is assumed to represent a single command, and each argument is
            quoted/escaped to ensure that special characters are properly handled.
        cpus: The number of CPUs to reserve. Must be a number in the range 1 to 128.
            Defaults to 1.
        memory: The amount of memory to reserve. Must be a positive number (in MB) or a
            string ending with a unit (K, M, G, T). Defaults to ~16G per CPU.
        gpus: The number of CPUs to reserve, either 0, 1, or 2. Jobs that reserve CPUs
            will be run on the GPU queue. Defaults to 0.
        gpu_type: Preferred GPU type, if any, either 'a100' or 'h100'. Defaults to None.
        job_name: An optional string naming the current Slurm job.
        modules: A list of zero or more environment modules to load before running the
            commands specified above. Defaults to ().
        extra_args: A list of arguments passed directly to srun/sbatch. Multi-part
            arguments must therefore be split into multiple values:
            ["--foo", "bar"] and not ["--foo bar"]
        output_file: Optional name of log-file foom the job.
        array_params: Optional job-array parameters (see "--array").
        mail_user: Send an email to user on failures or completion of the job. May
            either be an email address, or `True` to send an email to `$USER@ku.dk`.
        wait: If true, wait for the job to complete before returning. Defaults to False.
        strict: If true, the script is configured to terminate on the first error.
            Defaults to true.

    Returns:
        int: The JobID of the submitted job.

    """
    script = sbatch_script(
        commands,
        cpus=cpus,
        gpus=gpus,
        gpu_type=gpu_type,
        memory=memory,
        job_name=job_name,
        modules=modules,
        extra_args=extra_args,
        output_file=output_file,
        array_params=array_params,
        wait=wait,
        mail_user=mail_user,
        strict=strict,
    )

    with tempfile.NamedTemporaryFile("wt", encoding="utf-8") as handle:
        handle.writelines(script)
        handle.flush()

        # --parsable ensures that the job ID is easily retrieved
        with subprocess.Popen(
            [*SBATCH_CMD, "--parsable", handle.name],
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as process:
            stdout, stderr = process.communicate()

    if process.returncode == 0:
        output = stdout.decode().strip()
        job_id, *_cluster = output.split(";", 1)

        return int(job_id)

    raise SlurmError(stderr.decode().strip())


def srun_command(
    *,
    cpus: int = 1,
    gpus: int = 0,
    gpu_type: Literal["a100", "h100", "A100", "H100"] | None = None,
    memory: int | str | None = None,
    extra_args: SequenceNotStr[str] = (),
) -> list[str]:
    """Generate command-line arguments for an `srun` command.

    Args:
        cpus: The number of CPUs to reserve. Must be a number in the range 1 to 128.
            Defaults to 1.
        memory: The amount of memory to reserve. Must be a positive number (in MB) or a
            string ending with a unit (K, M, G, T). Defaults to ~16G per CPU.
        gpus: The number of CPUs to reserve, either 0, 1, or 2. Jobs that reserve CPUs
            will be run on the GPU queue. Defaults to 0.
        gpu_type: Preferred GPU type, if any, either 'a100' or 'h100'. Defaults to None.
        extra_args: A list of arguments passed directly to srun/sbatch. Multi-part
            arguments must therefore be split into multiple values:
            ["--foo", "bar"] and not ["--foo bar"]

    Returns:
        list[str]: A list of command-line arguments for `srun`.

    """
    return [
        *SRUN_CMD,
        *slurm_options(
            cpus=cpus,
            gpus=gpus,
            gpu_type=gpu_type,
            memory=memory,
        ),
        *extra_args,
    ]


class SrunResult(Generic[_T_out]):
    __slots__ = ["returncode", "stderr", "stdout"]

    returncode: int
    stdout: _T_out
    stderr: _T_out

    def __init__(self, *, returncode: int, stdout: _T_out, stderr: _T_out) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

    def __bool__(self) -> bool:
        return self.returncode == 0

    def __repr__(self) -> str:
        stdout = self._to_repr(self.stdout)
        stderr = self._to_repr(self.stderr)

        return (
            f"SrunResult(returncode={self.returncode},stdout={stdout},stderr={stderr})"
        )

    def __hash__(self) -> int:
        return hash((self.returncode, self.stdout, self.stderr))

    def __eq__(self, value: object) -> bool:
        if isinstance(value, SrunResult):
            # Assume same generic for simplicity
            value = cast("SrunResult[_T_out]", value)
            return (
                (self.returncode == value.returncode)
                and (self.stdout == value.stdout)
                and (self.stderr == value.stderr)
            )

        return NotImplemented

    def _to_repr(self, value: _T_out) -> str:
        if value is not None and len(value) > 13:
            return f"{value[:10]}..."

        return repr(value)


@overload
def srun(
    command: Sequence[str],
    *,
    cpus: int = ...,
    gpus: int = ...,
    memory: int | str | None = ...,
    modules: SequenceNotStr[str] = (),
    extra_args: SequenceNotStr[str] = ...,
    capture: Literal[False] = False,
) -> SrunResult[None]: ...


@overload
def srun(
    command: Sequence[str],
    *,
    cpus: int = ...,
    gpus: int = ...,
    memory: int | str | None = ...,
    modules: SequenceNotStr[str] = (),
    extra_args: SequenceNotStr[str] = ...,
    capture: Literal[True],
    text: Literal[True] = True,
) -> SrunResult[str]: ...


@overload
def srun(
    command: Sequence[str],
    *,
    cpus: int = ...,
    gpus: int = ...,
    memory: int | str | None = ...,
    modules: SequenceNotStr[str] = (),
    extra_args: SequenceNotStr[str] = ...,
    capture: Literal[True],
    text: Literal[False],
) -> SrunResult[bytes]: ...


def srun(
    command: Sequence[str],
    *,
    cpus: int = 1,
    gpus: int = 0,
    memory: int | str | None = None,
    modules: SequenceNotStr[str] = (),
    extra_args: SequenceNotStr[str] = (),
    capture: bool = False,
    text: bool = True,
    strict: bool = True,
) -> SrunResult[None] | SrunResult[str] | SrunResult[bytes]:
    """Run command via `srun`, and optionally capture its output.

    WARNING: This function can only be used from esrumhead01fl!

    Args:
        command: The command to run, either as a single string that is assumed to
            contain a properly formatted shell command, or as a list of strings, that is
            assumed to present each argument in the command.
        cpus: The number of CPUs to reserve. Must be a number in the range 1 to 128.
            Defaults to 1.
        memory: The amount of memory to reserve. Must be a positive number (in MB) or a
            string ending with a unit (K, M, G, T). Defaults to ~16G per CPU.
        gpus: The number of CPUs to reserve, either 0, 1, or 2. Jobs that reserve CPUs
            will be run on the GPU queue. Defaults to 0.
        gpu_type: Preferred GPU type, if any, either 'a100' or 'h100'. Defaults to None.
        extra_args: A list of arguments passed directly to srun/sbatch. Multi-part
            arguments must therefore be split into multiple values:
            ["--foo", "bar"] and not ["--foo bar"]
        modules: A list of zero or more environment modules to load before running the
            commands specified above. Defaults to ().
        capture: If true, srun's stdout and stderr is captured and returned. Defaults to
            False.
        text: If true, output captured by `capture` is assumed to be UTF8 and decoded
            to strings. Otherwise bytes are returned. Defaults to True.
        strict: If true, the script is configured to terminate on the first error.
            Defaults to true.

    Raises:
        SlurmError: Raised if this command is invoked on a compute node.

    Returns:
        int: The exit-code from running `srun` (non-zero on error)
        int, str, str: The srun exit-code, stdout, and stderr, if `capture` is True.
        int, bytes, bytes: As above, but `text` is False.

    """
    if not IS_HEAD_NODE:
        raise SlurmError("`srun` can only be called on the head node")

    # The user command is wrapped in a script to allow loading of modules
    script = sbatch_script(
        commands=[command],
        modules=modules,
        strict=strict,
    )

    with tempfile.NamedTemporaryFile(
        "wt",
        encoding="utf-8",
        # the temporary file must be accessible on the NFS filesystem
        prefix=".srun_",
        dir=Path.cwd(),
    ) as handle:
        handle.writelines(script)
        handle.flush()

        pipe = subprocess.PIPE if capture else None
        with subprocess.Popen(
            [
                *srun_command(
                    cpus=cpus,
                    gpus=gpus,
                    memory=memory,
                    extra_args=extra_args,
                ),
                SHELL_CMD,
                handle.name,
            ],
            shell=False,
            stdout=pipe,
            stderr=pipe,
            text=text,
        ) as process:
            stdout_, stderr_ = process.communicate()

    return SrunResult(returncode=process.returncode, stdout=stdout_, stderr=stderr_)


def _to_clean_str(value: object) -> str:
    """Clean a user-supplied value for use as a CLI argument."""
    svalue = value if isinstance(value, str) else str(value)
    svalue = svalue.strip()
    if "\n" in svalue:
        raise ValueError(f"value must not contain newlines: {value!r}")
    elif not svalue:
        raise ValueError(f"argument {value!r} is empty or contains only whitespace")

    return svalue


def _parse_memory(value: str | int) -> tuple[str, bool]:
    """Parse, validate, and normalize a `--mem` argument.

    Raises:
        ValueError: If the string or number is not a valid memory reservation.

    Returns:
        str: Normalized string for use with `--mem`
        bool: True if the memory reservation requires running on a high-memory node.

    """
    if isinstance(value, int) or value.isdigit():
        # make default behavior explicit
        value_ = f"{value}M"
    else:
        value_ = _to_clean_str(value).upper()

    if not (value_ and value_[:-1].isdigit() and value_.endswith(("K", "M", "G", "T"))):
        raise ValueError(f"invalid `memory` value {value!r}")

    memory = int(value_[:-1])
    unit = value_[-1]

    memory_kb = memory * (2 ** {"K": 0, "M": 10, "G": 20, "T": 30}[unit])
    if memory_kb <= 0:
        raise ValueError(f"non-positive `memory` value {value!r} not allowed")
    elif memory_kb > MAX_HIGH_MEMORY:
        raise ValueError(f"`memory` greater than high-memory node capacity: {value!r}")

    return value_, memory_kb > MAX_LOW_MEMORY


def _quote_commands(
    commands: SequenceNotStr[str] | Sequence[SequenceNotStr[str]],
) -> Iterator[str]:
    """Quote and merge arguments in commands given as lists of individual arguments."""
    for command in commands:
        if not isinstance(command, str):
            # Ensure that white-space and special characters are correctly quoted
            command = " ".join(shlex.quote(v) for v in command)

        if not command.endswith("\n"):
            command = f"{command}\n"

        yield command
