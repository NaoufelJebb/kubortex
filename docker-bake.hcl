// Docker Bake file — builds all four component images in parallel.
//
// Usage:
//   docker buildx bake                         # build all, load into local daemon
//   docker buildx bake --push                  # build + push to registry
//   docker buildx bake operator                # build one component only
//   REGISTRY=gcr.io/my-project TAG=v1.0 docker buildx bake --push

variable "REGISTRY" {
  default = "your-registry"
}

variable "TAG" {
  default = "latest"
}

group "default" {
  targets = ["operator", "edge", "investigator", "remediator"]
}

target "operator" {
  dockerfile = "Dockerfile"
  target     = "operator"
  tags       = ["${REGISTRY}/kubortex-operator:${TAG}"]
  platforms  = ["linux/amd64", "linux/arm64"]
}

target "edge" {
  dockerfile = "Dockerfile"
  target     = "edge"
  tags       = ["${REGISTRY}/kubortex-edge:${TAG}"]
  platforms  = ["linux/amd64", "linux/arm64"]
}

target "investigator" {
  dockerfile = "Dockerfile"
  target     = "investigator"
  tags       = ["${REGISTRY}/kubortex-investigator:${TAG}"]
  platforms  = ["linux/amd64", "linux/arm64"]
}

target "remediator" {
  dockerfile = "Dockerfile"
  target     = "remediator"
  tags       = ["${REGISTRY}/kubortex-remediator:${TAG}"]
  platforms  = ["linux/amd64", "linux/arm64"]
}
