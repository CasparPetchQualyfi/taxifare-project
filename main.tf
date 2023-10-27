provider "azurerm" {
  features {}
}

resource "azurerm_virtual_network" "network" {
  name                = "CP-vnet"
  address_space       = ["10.0.0.0/16"]
  location            = "North Europe"
  resource_group_name = "rg-data-cohort-labs"

  tags = {
    environment = "dev"
  }
}

# resource "azurerm_subnet" "subnet-1" {
#   name                 = "subnet-1"
#   resource_group_name  = azurerm_virtual_network.network.resource_group_name
#   virtual_network_name = azurerm_virtual_network.network.name
#   address_prefixes     = ["10.0.1.0/26"]
# }

# resource "azurerm_subnet" "subnet-2" {
#   name                 = "subnet-2"
#   resource_group_name  = azurerm_virtual_network.network.resource_group_name
#   virtual_network_name = azurerm_virtual_network.network.name
#   address_prefixes     = ["10.0.2.0/26"]
# }

# resource "azurerm_storage_account" "storage" {
#   name                     = "cpstrgccntqualyfi"
#   resource_group_name      = azurerm_virtual_network.network.resource_group_name
#   location                 = "North Europe"
#   account_tier             = "Standard"
#   account_replication_type = "LRS"

#   tags = {
#     environment = "dev"
#   }
# }

resource "azurerm_storage_container" "container" {
  count               = length(var.container_names)
  name                = var.container_names[count.index]
  storage_account_name = "datacohortworkspacelabs"
}

variable "container_names" {
  type    = list(string)
  default = ["landing-cp", "bronze-cp", "silver-cp", "gold-cp"]
}

