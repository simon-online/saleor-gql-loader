"""Implements a data loader that loads data into Saleor through graphQL.

Notes
-----
This module is designed and working with Saleor v3.14.0. Update will be necessary
for future release if the data models change.

No tests have been implemented as testing would need to create a fake db, which
requires a lot of dev better redo the project as a django app inside saleor
project for easier testing.

"""
from .utils import (
    graphql_request,
    graphql_multipart_request,
    override_dict,
    handle_errors,
    get_payload,
)


class ETLDataLoader:
    """abstraction around several graphQL query to load data into Saleor.

    Notes
    -----
    This class requires a valid `auth_token` to be provided during
    initialization. An `app` must be first created for example using django cli

    ```bash
    python manage.py create_app etl --permission account.manage_users \
                                    --permission account.manage_staff \
                                    --permission app.manage_apps \
                                    --permission app.manage_apps \
                                    --permission discount.manage_discounts \
                                    --permission plugins.manage_plugins \
                                    --permission giftcard.manage_gift_card \
                                    --permission menu.manage_menus \
                                    --permission order.manage_orders \
                                    --permission page.manage_pages \
                                    --permission product.manage_products \
                                    --permission shipping.manage_shipping \
                                    --permission site.manage_settings \
                                    --permission site.manage_translations \
                                    --permission webhook.manage_webhooks \
                                    --permission checkout.manage_checkouts
    ```

    Attributes
    ----------
    headers : dict
        the headers used to make graphQL queries.
    endpoint_url : str
        the graphQL endpoint url to query to.

    Methods
    -------

    """

    def __init__(
        self,
        auth_token=None,
        email=None,
        password=None,
        endpoint_url="http://localhost:8000/graphql/",
    ):
        """initialize the `DataLoader` with an auth_token and an url endpoint.

        Parameters
        ----------
        auth_token : str
            token used to identify called to the graphQL endpoint.
        email : str
            email to authenticate with
        password : str
            password to authenticate with
        endpoint_url : str, optional
            the graphQL endpoint to be used , by default "http://localhost:8000/graphql/"
        """
        self.email = None
        self.password = None
        self.headers = {}
        self.endpoint_url = endpoint_url

        if email and password:
            self.authenticate(email, password)
        elif auth_token:
            self.set_auth_header(auth_token)

    def set_auth_header(self, auth_token):
        if auth_token:
            self.headers["Authorization"] = "Bearer {}".format(auth_token)
        else:
            raise Exception("Authentication failed - check details are correct")

    def authenticate(self, email=None, password=None):
        if email:
            self.email = email

        if password:
            self.password = password

        variables = {"email": self.email, "password": self.password}

        query = """
            mutation createToken($email: String!, $password: String!) {
                tokenCreate(email: $email, password: $password) {
                    token
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response)

        self.set_auth_header(response["data"]["tokenCreate"]["token"])

    def update_shop_settings(self, **kwargs):
        """update shop settings.

        Parameters
        ----------
        **kwargs : dict, optional
            overrides the default value set to update the shop settings refer to the
            ShopSettingsInput graphQL type to know what can be overriden.

        Raises
        ------
        Exception
            when errors is not an empty list
        """

        variables = {"input": kwargs}

        query = """
            mutation ShopSettingsUpdate($input: ShopSettingsInput!) {
              shopSettingsUpdate(input: $input) {
                shop {
                    headerText
                    description
                    includeTaxesInPrices
                    displayGrossPrices
                    chargeTaxesOnShipping
                    trackInventoryByDefault
                    defaultWeightUnit
                    automaticFulfillmentDigitalProducts
                    defaultDigitalMaxDownloads
                    defaultDigitalUrlValidDays
                    defaultMailSenderName
                    defaultMailSenderAddress
                    customerSetPasswordUrl
                }
                errors {
                    field
                    message
                    code
                }
              }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "shopSettingsUpdate", "errors"))

        return response["data"]["shopSettingsUpdate"]["shop"]

    def update_shop_domain(self, **kwargs):
        """update shop domain.

        Parameters
        ----------
        **kwargs : dict, optional
            overrides the default value set to update the shop domain refer to the
            SiteDomainInput graphQL type to know what can be overriden.

        Returns
        -------
        domain : dict
            the new shop domain

        Raises
        ------
        Exception
            when errors is not an empty list
        """

        variables = {"siteDomainInput": kwargs}

        query = """
            mutation ShopDomainUpdate($siteDomainInput: SiteDomainInput!) {
              shopDomainUpdate(input: $siteDomainInput) {
                shop {
                    domain {
                        host
                        sslEnabled
                        url
                    }
                }
                errors {
                    field
                    message
                    code
                }
              }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "shopDomainUpdate", "errors"))

        return response["data"]["shopDomainUpdate"]["shop"]["domain"]

    def update_shop_address(self, **kwargs):
        """update shop address.

        Parameters
        ----------
        **kwargs : dict, optional
            overrides the default value set to update the shop address refer to the
            AddressInput graphQL type to know what can be overriden.

        Returns
        -------
        companyAddress : dict
            the new shop address

        Raises
        ------
        Exception
            when errors is not an empty list
        """

        variables = {"addressInput": kwargs}

        query = """
            mutation ShopAddressUpdate($addressInput: AddressInput!) {
              shopAddressUpdate(input: $addressInput) {
                shop {
                    companyAddress {
                        id
                        firstName
                        lastName
                        companyName
                        streetAddress1
                        streetAddress2
                        city
                        cityArea
                        postalCode
                        country {
                            code
                            country
                        }
                        countryArea
                        phone
                        isDefaultShippingAddress
                        isDefaultBillingAddress
                    }
                }
                errors {
                    field
                    message
                    code
                }
              }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "shopAddressUpdate", "errors"))

        return response["data"]["shopAddressUpdate"]["shop"]["companyAddress"]

    def create_channel(self, **kwargs):
        """create a channel.

        Parameters
        ----------
        **kwargs : dict, optional
            overrides the default value set to create the channel. Refer to the
            ChannelCreateInput graphQL type to know what can be overriden.

        Returns
        -------
        id : str
            the id of the channel created

        Raises
        ------
        Exception
            when errors is not an empty list
        """
        default_kwargs = {
            "isActive": True,
            "name": "Fake Channel",
            "slug": "fake-channel",
            "currencyCode": "USD",
            "defaultCountry": "US",
            "addShippingZones": [],
        }

        override_dict(default_kwargs, kwargs)

        variables = {"input": default_kwargs}

        query = """
            mutation createChannel($input: ChannelCreateInput!) {
                channelCreate(input: $input) {
                    channel {
                        id
                    }
                    errors {
                        field
                        message
                        code
                    }
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "channelCreate", "errors"))

        return response["data"]["channelCreate"]["channel"]["id"]

    def create_warehouse(self, **kwargs):
        """create a warehouse.

        Parameters
        ----------
        **kwargs : dict, optional
            overrides the default value set to create the warehouse refer to the
            WarehouseCreateInput graphQL type to know what can be overriden.

        Returns
        -------
        id : str
            the id of the warehouse created

        Raises
        ------
        Exception
            when errors is not an empty list
        """
        default_kwargs = {
            "email": "fake@example.com",
            "name": "Fake Warehouse",
            "address": {
                "companyName": "The Fake Company",
                "streetAddress1": "A Fake Street Address",
                "city": "Fake City",
                "postalCode": "1024",
                "country": "CH",
            },
        }

        override_dict(default_kwargs, kwargs)

        variables = {"input": default_kwargs}

        query = """
            mutation createWarehouse($input: WarehouseCreateInput!) {
                createWarehouse(input: $input) {
                    warehouse {
                        id
                    }
                    errors {
                        field
                        message
                        code
                    }
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "createWarehouse", "errors"))

        return response["data"]["createWarehouse"]["warehouse"]["id"]

    def create_shipping_zone(self, **kwargs):
        """create a shippingZone.

        Parameters
        ----------
        **kwargs : dict, optional
            overrides the default value set to create the shippingzone refer to
            the shippingZoneCreateInput graphQL type to know what can be
            overriden.

        Returns
        -------
        id : str
            the id of the shippingZone created.

        Raises
        ------
        Exception
            when errors is not an empty list.
        """
        default_kwargs = {
            "name": "CH",
            "countries": ["CH"],
            "default": False,
        }

        override_dict(default_kwargs, kwargs)

        variables = {"input": default_kwargs}

        query = """
            mutation createShippingZone($input: ShippingZoneCreateInput!) {
                shippingZoneCreate(input: $input) {
                    shippingZone {
                        id
                    }
                    errors {
                        field
                        message
                        code
                    }
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "shippingZoneCreate", "errors"))

        return response["data"]["shippingZoneCreate"]["shippingZone"]["id"]

    def create_attribute(self, **kwargs):
        """create a product attribute.

        Parameters
        ----------
        **kwargs : dict, optional
            overrides the default value set to create the attribute refer to
            the AttributeCreateInput graphQL type to know what can be
            overriden.

        Returns
        -------
        id : str
            the id of the attribute created.

        Raises
        ------
        Exception
            when errors is not an empty list.
        """
        default_kwargs = {
            "inputType": "DROPDOWN",
            "name": "default",
            "type": "PRODUCT_TYPE",
        }

        override_dict(default_kwargs, kwargs)

        variables = {"input": default_kwargs}

        query = """
            mutation createAttribute($input: AttributeCreateInput!) {
                attributeCreate(input: $input) {
                    attribute {
                        id
                    }
                    errors {
                        field
                        message
                        code
                    }
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "attributeCreate", "errors"))

        return response["data"]["attributeCreate"]["attribute"]["id"]

    def create_attribute_value(self, attribute_id, **kwargs):
        """create a product attribute value.

        Parameters
        ----------
        attribute_id : str
            id of the attribute on which to add the value.
        **kwargs : dict, optional
            overrides the default value set to create the attribute refer to
            the AttributeValueCreateInput graphQL type to know what can be
            overriden.

        Returns
        -------
        id : str
            the id of the attribute on which the value was created.

        Raises
        ------
        Exception
            when errors is not an empty list.
        """
        default_kwargs = {"name": "default"}

        override_dict(default_kwargs, kwargs)

        variables = {"attribute": attribute_id, "input": default_kwargs}

        query = """
            mutation createAttributeValue($input: AttributeValueCreateInput!, $attribute: ID!) {
                attributeValueCreate(input: $input, attribute: $attribute) {
                    attribute{
                        id
                    }
                    errors {
                        field
                        message
                        code
                    }
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "attributeValueCreate", "errors"))

        return response["data"]["attributeValueCreate"]["attribute"]["id"]

    def create_product_type(self, **kwargs):
        """create a product type.

        Parameters
        ----------
        **kwargs : dict, optional
            overrides the default value set to create the type refer to
            the ProductTypeInput graphQL type to know what can be
            overriden.

        Returns
        -------
        id : str
            the id of the productType created.

        Raises
        ------
        Exception
            when errors is not an empty list.
        """
        default_kwargs = {
            "name": "default",
            "hasVariants": False,
            "productAttributes": [],
            "variantAttributes": [],
            "isDigital": "false",
        }

        override_dict(default_kwargs, kwargs)

        variables = {"input": default_kwargs}

        query = """
            mutation createProductType($input: ProductTypeInput!) {
                productTypeCreate(input: $input) {
                    productType {
                        id
                    }
                    errors {
                        field
                        message
                        code
                    }
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "productTypeCreate", "errors"))

        return response["data"]["productTypeCreate"]["productType"]["id"]

    def create_category(self, parent_id, **kwargs):
        """create a category.

        Parameters
        ----------
        parent_id : str
            the parent product category id or empty if top level category
        **kwargs : dict, optional
            overrides the default value set to create the category refer to
            the CategoryInput graphQL type to know what can be
            overriden.

        Returns
        -------
        id : str
            the id of the product category created.

        Raises
        ------
        Exception
            when errors is not an empty list.
        """

        default_kwargs = {"name": "default"}

        override_dict(default_kwargs, kwargs)

        variables = {"input": default_kwargs, "parent": parent_id}

        query = """
            mutation createCategory($input: CategoryInput!, $parent: ID) {
                categoryCreate(input: $input, parent: $parent) {
                    category {
                        id
                    }
                    errors {
                        field
                        message
                        code
                    }
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "categoryCreate", "errors"))

        return response["data"]["categoryCreate"]["category"]["id"]

    def create_product(self, product_type_id, **kwargs):
        """create a product.

        Parameters
        ----------
        product_type_id : str
            product type id required to create the product.
        **kwargs : dict, optional
            overrides the default value set to create the product refer to
            the ProductCreateInput graphQL type to know what can be
            overriden.

        Returns
        -------
        id : str
            the id of the product created.

        Raises
        ------
        Exception
            when errors is not an empty list.
        """
        default_kwargs = {"name": "default", "productType": product_type_id}

        override_dict(default_kwargs, kwargs)

        variables = {"input": default_kwargs}

        query = """
            mutation createProduct($input: ProductCreateInput!) {
                productCreate(input: $input) {
                    product {
                        id
                    }
                    errors {
                        field
                        message
                        code
                    }
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "productCreate", "errors"))

        return response["data"]["productCreate"]["product"]["id"]

    def create_product_variant(self, product_id, **kwargs):
        """create a product variant.

        Parameters
        ----------
        product_id : str
            id for which the product variant will be created.
        **kwargs : dict, optional
            overrides the default value set to create the product variant refer
            to the ProductVariantCreateInput graphQL type to know what can be
            overriden.

        Returns
        -------
        id : str
            the id of the product variant created.

        Raises
        ------
        Exception
            when errors is not an empty list.
        """
        default_kwargs = {"product": product_id, "sku": "0", "attributes": []}

        override_dict(default_kwargs, kwargs)

        variables = {"input": default_kwargs}

        query = """
            mutation createProductVariant($input: ProductVariantCreateInput!) {
                productVariantCreate(input: $input) {
                    productVariant {
                        id
                    }
                    errors {
                        field
                        message
                        code
                    }
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "productVariantCreate", "errors"))

        return response["data"]["productVariantCreate"]["productVariant"]["id"]

    def create_product_media(self, product_id, file_path=None, file_url=None, alt=""):
        """create a product media.

        Parameters
        ----------
        product_id : str
            id for which the product image will be created.
        file_path : str
            path to the image to upload.
        file_url : str
            url to the media file to add
        alt : str
            alt description for the media

        Returns
        -------
        id : str
            the id of the product image created.

        Raises
        ------
        Exception
            when errors is not an empty list.
        """
        if file_path:
            body = get_payload(product_id, file_path, alt)

            response = graphql_multipart_request(body, self.headers, self.endpoint_url)
        else:
            kwargs = {"product": product_id, "mediaUrl": file_url}

            if alt:
                kwargs["alt"] = alt

            variables = {"input": kwargs}

            query = """
                mutation createProductMedia($input: ProductMediaCreateInput!) {
                    productMediaCreate(input: $input) {
                        media {
                            id
                        }
                        errors {
                            field
                            message
                        }
                    }
                }
            """

            response = graphql_request(
                query, variables, self.headers, self.endpoint_url
            )

        handle_errors(response, ("data", "productMediaCreate", "errors"))

        return response["data"]["productMediaCreate"]["media"]["id"]

    def create_customer_account(self, **kwargs):
        """
        Creates a customer (as an admin)
        Parameters
        ----------
        kwargs: customer

        Returns
        -------

        """
        default_kwargs = {
            "firstName": "default",
            "lastName": "default",
            "email": "default@default.com",
            "isActive": False,
        }

        override_dict(default_kwargs, kwargs)

        variables = {"input": default_kwargs}

        query = """
            mutation customerCreate($input: UserCreateInput !) {
                customerCreate(input: $input) {
                    user {
                        id
                    }
                    errors {
                        field
                        message
                        code
                    }
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "customerCreate", "errors"))

        return response["data"]["customerCreate"]["user"]["id"]

    def find_customer_by_email(self, email):
        """
        Finds a customer by email
        Parameters
        ----------
        email: str

        Returns
        -------
        id: str
        """
        variables = {"email": email}

        query = """
            query customerByEmail($email: String!) {
                user(email: $email) {
                    id
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "user"))
        if not response["data"]["user"]:
            return None
        return response["data"]["user"]["id"]

    def delete_customer_account(self, customer_id):
        """
        Deletes a customer (as an admin)
        Parameters
        ----------
        customer_id: str

        Returns
        -------
        id: str
            the user id of the deleted customer

        Raises
        ------
        Exception
            when errors is not an empty list.
        """
        variables = {"id": customer_id}

        query = """
            mutation CustomerDelete($id: ID!) {
                customerDelete(id: $id) {
                    user {
                        id
                    }
                    errors {
                        field
                        message
                        code
                    }
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "customerDelete", "errors"))

        return response["data"]["customerDelete"]["user"]["id"]

    def delete_customer_accounts(self, customer_ids):
        """
        Deletes multiple customers (as an admin)
        Parameters
        ----------
        customer_ids: list

        Returns
        -------
        count: int
            the count of deleted customers

        Raises
        ------
        Exception
            when errors is not an empty list.
        """
        variables = {'ids': customer_ids}

        query = """
            mutation CustomersDelete($ids: [ID!]!) {
                customerBulkDelete(ids: $ids) {
                    count
                    errors {
                        field
                        message
                        code
                    }
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "customerBulkDelete", "errors"))

        return response["data"]["customerBulkDelete"]["count"]

    def update_product(self, product_id, input_data):
        """update a product.
        Use this to set product fields and attributes.

        Parameters
        ----------
        product_id : str
            product id to update.
        input_data : dict
            the product fields and attributes data.

        Returns
        -------
        id : str
            the id of the updated product.

        Raises
        ------
        Exception
            when errors is not an empty list.
        """

        variables = {"id": product_id, "input": input_data}

        query = """
            mutation updateProductChannelListings($id: ID!, $input: ProductInput!) {
                productUpdate(id: $id, input: $input) {
                    product {
                        id
                    }
                    errors {
                        field
                        message
                        code
                    }
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "productUpdate", "errors"))

        return response["data"]["productUpdate"]["product"]["id"]

    def update_product_channel_listings(self, product_id, input_data):
        """update a product channel listings.
        Use this to set product and variant channel availability.

        Parameters
        ----------
        product_id : str
             product id to update channel availability.
        input_data : dict
            the product and variant channel availability data.

        Returns
        -------
        id : str
            the id of the updated product.

        Raises
        ------
        Exception
            when errors is not an empty list.
        """

        variables = {"id": product_id, "input": input_data}

        query = """
            mutation updateProductChannelListings($id: ID!, $input: ProductChannelListingUpdateInput!) {
                productChannelListingUpdate(id: $id, input: $input) {
                    product {
                        id
                    }
                    errors {
                        field
                        message
                        code
                    }
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(
            response,
            ("data", "productChannelListingUpdate", "errors"),
        )

        return response["data"]["productChannelListingUpdate"]["product"]["id"]

    def update_product_variant_channel_listings(self, product_variant_id, input_list):
        """update a product variant channel listings.
        Use to set product variant prices for each channel including for simple products which just have
        one product variant.

        Parameters
        ----------
        product_variant_id : str
             product variant id for price updates.
        input_list : list
            all the channel pricing details for a product variant. refer
            to the ProductVariantChannelListingAddInput graphQL type for more details.

        Returns
        -------
        id : str
            the id of the updated product variant.

        Raises
        ------
        Exception
            when errors is not an empty list.
        """

        variables = {"id": product_variant_id, "input": input_list}

        query = """
            mutation updateProductVariantChannelListings($id: ID!, $input: [ProductVariantChannelListingAddInput!]!) {
                productVariantChannelListingUpdate(id: $id, input: $input) {
                    variant {
                        id
                    }
                    errors {
                        field
                        message
                        code
                    }
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(
            response,
            (
                "data",
                "productVariantChannelListingUpdate",
                "errors",
            ),
        )

        return response["data"]["productVariantChannelListingUpdate"]["variant"]["id"]

    def update_product_variant_stocks(self, product_variant_id, stocks):
        """update the stock levels for a product variant.
        Use to set warehouse stock levels for a product variant including for simple products which just have
        one product variant.

        Parameters
        ----------
        product_variant_id : str
             product variant id for stock updates.
        stocks : list
            all the channel stock details for a product variant. refer
            to the StockInput graphQL type for more details.

        Returns
        -------
        id : str
            the id of the updated product variant.

        Raises
        ------
        Exception
            when errors is not an empty list.
        """

        variables = {"variantId": product_variant_id, "stocks": stocks}

        query = """
            mutation updateProductVariantStocks($variantId: ID!, $stocks: [StockInput!]!) {
                productVariantStocksUpdate(variantId: $variantId, stocks: $stocks) {
                    productVariant {
                        id
                    }
                    errors {
                        field
                        message
                        code
                    }
                }
            }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(
            response, ("data", "productVariantStocksUpdate", "errors")
        )

        return response["data"]["productVariantStocksUpdate"]["productVariant"]["id"]

    def update_public_meta(self, item_id, input_list):
        """
        Parameters
        ----------
        item_id: ID of the item to update. Model needs to work with public metadata
        input_list: an input dict to which to set the public meta

        Returns
        -------
        Item ID if successful, None if not
        """
        variables = {"id": item_id, "input": input_list}

        query = """
                    mutation updateMetadata($id: ID!, $input: [MetadataInput!]!) {
                        updateMetadata(id: $id, input: $input) {
                            item {
                                metadata {
                                    key
                                    value
                                }
                            }
                            errors {
                                field
                                message
                                code
                            }
                        }
                    }
                """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "updateMetadata", "errors"))

        if len(response["data"]["updateMetadata"]["item"]["metadata"]) > 0:
            return item_id
        else:
            return None

    def update_private_meta(self, item_id, input_list):
        """

        Parameters
        ----------
        item_id: ID of the item to update. Model need to work with private metadata
        input_list: an input dict to which to set the private meta

        Returns
        -------
        Item ID if successful, None if not
        """

        variables = {"id": item_id, "input": input_list}

        query = """
                    mutation updatePrivateMetadata($id: ID!, $input: [MetadataInput!]!) {
                        updatePrivateMetadata(id: $id, input: $input) {
                            item {
                                privateMetadata {
                                    key
                                    value
                                }
                            }
                            metadataErrors {
                                field
                                message
                                code
                            }
                        }
                    }
                """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "updatePrivateMetadata", "errors"))

        if (
            len(response["data"]["updatePrivateMetadata"]["item"]["privateMetadata"])
            > 0
        ):
            return item_id
        else:
            return None

    def update_plugin(self, plugin_id, channel_id=None, **kwargs):

        variables = {"id": plugin_id, "channel": channel_id, "input": kwargs}

        query = """
                    mutation PluginUpdate($channel: ID, $id: ID!, $input: PluginUpdateInput!) {
                        pluginUpdate(channelId: $channel, id: $id, input: $input) {
                            plugin {
                                id
                                name
                                channelConfigurations {
                                    active
                                    channel {
                                        id
                                        slug
                                        name
                                    }
                                    configuration {
                                        name
                                        value
                                        type
                                    }
                                }
                            }
                            errors {
                                field
                                message
                                code
                            }
                        }
                    }
                """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response, ("data", "pluginUpdate", "errors"))

        return response["data"]["pluginUpdate"]["plugin"]

    def fetch_channels(self):
        variables = {}
        query = """
        query FetchAllChannels {
            channels {
                id,
                name,
                slug
            }
        }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response)

        return response["data"]["channels"]

    def fetch_warehouses(self):
        warehouses = []

        variables = {"after": ""}
        query = """
        query FetchAllWarehouses($after: String) {
            warehouses(first: 10, after: $after) {
                pageInfo {
                  hasNextPage,
                  endCursor
                }
                edges {
                    node {
                        id,
                        name,
                        slug
                    }
                }
            }
        }
        """

        has_next_page = True

        while has_next_page:
            response = graphql_request(
                query, variables, self.headers, self.endpoint_url
            )

            handle_errors(response)

            warehouse_data = response["data"]["warehouses"]

            for edge in warehouse_data["edges"]:
                warehouses.append(edge["node"])

            has_next_page = warehouse_data["pageInfo"]["hasNextPage"]
            variables["after"] = warehouse_data["pageInfo"]["endCursor"]

        return warehouses

    def fetch_product_types(self):
        product_types = []

        variables = {"after": ""}
        query = """
        query FetchAllProductTypes($after: String) {
            productTypes(filter: {kind: NORMAL}, first: 10, after: $after) {
                pageInfo {
                  hasNextPage,
                  endCursor
                }
                edges {
                    node {
                        id,
                        name,
                        slug,
                        productAttributes {
                            id,
                            name,
                            slug
                        }
                        variantAttributes {
                            id,
                            name,
                            slug
                        }
                    }
                }
            }
        }
        """

        has_next_page = True

        while has_next_page:
            response = graphql_request(
                query, variables, self.headers, self.endpoint_url
            )

            handle_errors(response)

            types = response["data"]["productTypes"]

            for edge in types["edges"]:
                product_types.append(edge["node"])

            has_next_page = types["pageInfo"]["hasNextPage"]
            variables["after"] = types["pageInfo"]["endCursor"]

        return product_types

    def fetch_product_categories(self):
        product_categories = []

        level = 0
        variables = {"level": level, "after": ""}
        query = """
        query FetchAllProductCategories($level: Int, $after: String) {
            categories(level: $level, first: 10, after: $after) {
                pageInfo {
                  hasNextPage,
                  endCursor
                }
                edges {
                    node {
                        id,
                        name,
                        slug
                    }
                }
                totalCount
            }
        }
        """

        has_next_page = True

        while has_next_page:
            response = graphql_request(
                query, variables, self.headers, self.endpoint_url
            )

            handle_errors(response)

            categories = response["data"]["categories"]

            if categories["totalCount"]:
                for edge in categories["edges"]:
                    product_categories.append(edge["node"])

                has_next_page = categories["pageInfo"]["hasNextPage"]

                if has_next_page:
                    variables["after"] = categories["pageInfo"]["endCursor"]
                else:
                    level += 1
                    variables = {"level": level, "after": ""}
                    has_next_page = True
            else:
                has_next_page = False

        return product_categories

    def fetch_products(self, search=None):
        products = []

        filter_values = {}

        if search:
            filter_values["search"] = str(search)

        variables = {"filter": filter_values, "after": ""}

        query = """
        query FetchProducts($filter: ProductFilterInput, $after: String) {
            products(filter: $filter, first: 100, after: $after) {
                pageInfo {
                  hasNextPage,
                  endCursor
                }
                edges {
                    node {
                        id
                        name
                        slug
                        description
                        productType {
                            slug
                        }
                        attributes {
                            attribute {
                                id
                                slug
                            }
                            values {
                                name
                                slug
                                inputType
                                value
                                richText
                                plainText
                                boolean
                                date
                                dateTime
                            }
                        }
                        variants {
                            sku
                        }
                    }
                }
            }
        }
        """

        has_next_page = True

        while has_next_page:
            response = graphql_request(
                query, variables, self.headers, self.endpoint_url
            )

            handle_errors(response)

            data_products = response["data"]["products"]

            for edge in data_products["edges"]:
                products.append(edge["node"])

            has_next_page = data_products["pageInfo"]["hasNextPage"]
            variables["after"] = data_products["pageInfo"]["endCursor"]

        return products

    def fetch_product_variant(self, id=None, sku=None):
        variables = {"id": "", "sku": ""}

        if id:
            variables["id"] = id

        if sku:
            variables["sku"] = sku

        query = """
        query FetchProductVariant($id: ID, $sku: String) {
            productVariant(id: $id, sku: $sku) {
                id
                name
                sku
                product {
                    id
                    name
                }
            }
        }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response)

        return response["data"]["productVariant"]

    def fetch_customers(self, search=None):
        customers = []

        filter_values = {}

        if search:
            filter_values['search'] = str(search)

        variables = {'filter': filter_values, 'after': ''}

        query = """
        query FetchCustomers($filter: CustomerFilterInput, $after: String) {
            customers(filter: $filter, first: 100, after: $after) {
                pageInfo {
                  hasNextPage,
                  endCursor
                }
                edges {
                    node {
                        id
                        email
                        firstName
                        lastName
                        isStaff
                        isActive
                        isConfirmed
                    }
                }
            }
        }
        """

        has_next_page = True

        while has_next_page:
            response = graphql_request(
                query, variables, self.headers, self.endpoint_url
            )

            handle_errors(response)

            data_products = response['data']['customers']

            for edge in data_products['edges']:
                customers.append(edge['node'])

            has_next_page = data_products['pageInfo']['hasNextPage']
            variables['after'] = data_products['pageInfo']['endCursor']

        return customers

    def fetch_attribute(self, id=None, slug=None):
        variables = {"id": "", "slug": ""}

        if id:
            variables["id"] = id

        if slug:
            variables["slug"] = slug

        query = """
        query FetchAttribute($id: ID, $slug: String) {
            attribute(id: $id, slug: $slug) {
                id
                name
                slug
                inputType
            }
        }
        """

        response = graphql_request(query, variables, self.headers, self.endpoint_url)

        handle_errors(response)

        return response["data"]["attribute"]
