"""Module to define some utils non related to business logic.

Notes
-----
The function defined here must be context and implementation independant, for
easy reusability
"""
import mimetypes
import requests
import json
from pathlib import Path
from requests_toolbelt import MultipartEncoder
from django.core.serializers.json import DjangoJSONEncoder

GQL_DEFAULT_ENDPOINT = "http://localhost:8000/graphql/"


def graphql_request(query, variables={}, headers={},
                    endpoint=GQL_DEFAULT_ENDPOINT):
    """Execute the graphQL `query` provided on the `endpoint`.

    Parameters
    ----------
    query : str
        docstring representing a graphQL query.
    variables : dict, optional
        dictionary corresponding to the input(s) of the `query` must be
        serializable by requests into a JSON object.
    headers : dict, optional
        headers added to the request (important for authentication).
    endpoint : str, optional
        the graphQL endpoint url that will be queried, default is
        `GQL_DEFAULT_ENDPOINT`.

    Returns
    -------
    response : dict
        a dictionary corresponding to the parsed JSON graphQL response.

    Raises
    ------
    Exception
        when `response.status_code` is not 200.
    """
    response = requests.post(
        endpoint,
        headers=headers,
        json={
            'query': query,
            'variables': variables
        }
    )

    parsed_response = json.loads(response.text)
    if response.status_code != 200:
        raise Exception("{message}\n extensions: {extensions}".format(
            **parsed_response["errors"][0]))
    else:
        return parsed_response


def graphql_multipart_request(body, headers, endpoint=GQL_DEFAULT_ENDPOINT):
    """Execute a multipart graphQL query with `body` provided on the `endpoint`.

    Parameters
    ----------
    body : str
        payloads of graphQL query.
    headers : dict, optional
        headers added to the request (important for authentication).
    endpoint : str, optional
        the graphQL endpoint url that will be queried, default is
        `GQL_DEFAULT_ENDPOINT`.

    Returns
    -------
    response : dict
        a dictionary corresponding to the parsed JSON graphQL response.

    Raises
    ------
    Exception
        when `response.status_code` is not 200.
    """
    bodyEncoder = MultipartEncoder(body)
    base_headers = {
        "Content-Type": bodyEncoder.content_type,
    }
    override_dict(base_headers, headers)

    response = requests.post(endpoint, data=bodyEncoder, headers=base_headers, timeout=90)

    parsed_response = json.loads(response.text)
    if response.status_code != 200:
        raise Exception("{message}\n extensions: {extensions}".format(
            **parsed_response["errors"][0]))
    else:
        return parsed_response


def override_dict(a, overrides):
    """Override a dict with another one **only first non nested keys**.

    Notes
    -----
    This works only with non-nested dict. If dictionarries are nested then the
    nested dict needs to be completly overriden.
    The Operation is performed inplace.

    Parameters
    ----------
    a : dict
        a dictionary to merge.
    overrides : dict
        another dictionary to merge.
    """
    for key, val in overrides.items():
        try:
            if type(a[key]) == dict:
                print(
                    "**warning**: key '{}' contained a dict make sure to override each value in the nested dict.".format(key))
        except KeyError:
            pass
        a[key] = val


def handle_errors(response, errors_path=None):
    """Handle a list of generic and request specific errors.

    Parameters
    ----------
    response : dict
        the entire response dict
    errors_path : tuple
        a dot notation path to look for a request specific errors list
        where each error must be a dict with at least the following
        keys: `field` and `message`

    Raises
    ------
    Exception
        when the request specific errors list is not empty and display {field} : {message} errors
        or generic errors list is not empty and display {message} errors
    """
    txt_list = None

    if errors_path:
        errors_path_found = True
        path_target = response

        for field in errors_path:
            if field in path_target and path_target[field]:
                path_target = path_target[field]
            else:
                errors_path_found = False
                break

        if errors_path_found:
            txt_list = [
                "{field} : {message}".format(**error) for error in path_target if 'field' in error]

    if not txt_list and 'errors' in response and response['errors']:
        txt_list = [
            error['message'] for error in response['errors'] if 'message' in error]

    if txt_list:
        raise Exception("\n".join(txt_list))

def get_operations(product_id, alt=''):
    """Get ProductMediaCreate operations

    Parameters
    ----------
    product_id : str
        id for which the product image will be created
    alt : str
        alt description for the product image

    Returns
    -------
    query : str
    variables: dict
    """
    query = """
        mutation ProductMediaCreate($product: ID!, $image: Upload!, $alt: String) {
            productMediaCreate(input: {alt: $alt, image: $image, product: $product}) {
                media {
                    id
                }
                productErrors {
                    field
                    message
                }
            }
        }
    """
    variables = {
        "product": product_id,
        "image": "0",
        "alt": alt
    }
    return {"query": query, "variables": variables}

def get_payload(product_id, file_path, alt=''):
    """Get ProductMediaCreate operations

    Parameters
    ----------
    product_id : str
        id for which the product media will be created
    alt : str
        alt description for the product image

    Returns
    -------
    query : str
    variables: dict
    """

    mime_type, encoding = mimetypes.guess_type(file_path)

    return {
        "operations": json.dumps(
            get_operations(product_id, alt), cls=DjangoJSONEncoder
        ),
        "map": json.dumps({'0': ["variables.image"]}, cls=DjangoJSONEncoder),
        "0": (Path(file_path).name, open(file_path, 'rb'), mime_type)
    }
