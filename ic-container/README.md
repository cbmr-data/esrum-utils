# Container for `ic`,a post-Imputation data checking program

Podman/singularity container and wrapper script for running `ic`:

<https://www.chg.ox.ac.uk/~wrayner/tools/Post-Imputation.html>

## Building container

Building the container requires

Use the included makefile to build the container, save it to a file:

```console
make build save
```

The resulting image must be converted to a singularity container:

```console
module load singularity
make convert
```

## Running the singularity container

The supplied wrapper scripts expects that the singularity image (`*.sif`) is located in a `build` subfolder relative to the location of the script itself.

If that is the case, then simply run

``` console
module load singularity
./ic_fond -h
```
