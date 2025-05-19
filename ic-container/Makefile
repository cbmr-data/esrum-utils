.PHONY: build save convert

TAG := v3

build:
	podman build -t "ic_fond:$(TAG)" .

save:
	mkdir -p "build"
	rm -f "build/ic_fond-$(TAG).tar.gz"
	podman save -o "build/ic_fond-$(TAG).tar.gz" "ic_fond:$(TAG)"

convert:
	rm -f "build/ic_fond-$(TAG).sif"
	singularity build "build/ic_fond-$(TAG).sif" "docker-archive://build/ic_fond-$(TAG).tar.gz"
