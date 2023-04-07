## Configure and provision GCP resources

Before using Terraform, a service account file is needed. The path of this file can be placed in [variables.tf](https://github.com/dherzey/bechdel-movies-project/blob/main/terraform/variables.tf) within the following lines:
```
variable "credentials" {
  description = "Location of service account credential file"
  default = "path/to/service/account/key.json"
  type = string
}
```
Alternatively, we can just delete the credentials in the [main.tf](https://github.com/dherzey/bechdel-movies-project/blob/main/terraform/main.tf) file and just set the service account through the command line:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"

# Refresh token/session, and verify authentication
gcloud auth application-default login
```

Finally, the following are the execution steps to run this folder in Terraform:

1. `terraform init`: initialize and configure backend
2. `terraform plan`: previews changes and proposes execution plan
3. `terraform apply`: asks for approval to execute plan and apply changes in the cloud
4. `terraform destroy` (if needed): removes resources from the cloud 

If successful, these files will create the following resources in GCP:
- Google Clous Storage Bucket
- BigQuery dataset