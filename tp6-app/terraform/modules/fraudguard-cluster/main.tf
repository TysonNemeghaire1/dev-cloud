# Module abstrait multi-provider pour le cluster FraudGuard
variable "cloud_provider" {
  type = string
  validation {
    condition     = contains(["gcp", "aws"], var.cloud_provider)
    error_message = "cloud_provider doit être 'gcp' ou 'aws'."
  }
}

variable "cluster_name" {
  type    = string
  default = "fraudguard-cluster"
}

variable "region" {
  type = string
  # europe-west9 pour GCP, eu-west-3 pour AWS
}

# Cluster GKE (si GCP)
resource "google_container_cluster" "fraudguard" {
  count    = var.cloud_provider == "gcp" ? 1 : 0
  name     = var.cluster_name
  location = var.region

  initial_node_count = 3
  node_config {
    machine_type = "e2-standard-4"
  }
}

# Cluster EKS (si AWS)
resource "aws_eks_cluster" "fraudguard" {
  count    = var.cloud_provider == "aws" ? 1 : 0
  name     = var.cluster_name
  role_arn = aws_iam_role.eks.arn

  vpc_config {
    subnet_ids = aws_subnet.eks[*].id
  }
}

output "cluster_endpoint" {
  value = var.cloud_provider == "gcp" ? (
    google_container_cluster.fraudguard[0].endpoint
    ) : (
    aws_eks_cluster.fraudguard[0].endpoint
  )
}
