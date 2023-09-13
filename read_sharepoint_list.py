from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.listitem_collection import ListItemCollection
import pandas as pd

def read_sharepoint_list(site_url, client_id, client_secret, list_name):
    """
    Reads a SharePoint list and converts it to a Pandas DataFrame.
    :param site_url: SharePoint site URL
    :param client_id: Client ID for authentication
    :param client_secret: Client secret for authentication
    :param list_name: Name of the SharePoint list to read
    :return: Pandas DataFrame containing the SharePoint list data
    """
    try:
        context_auth = AuthenticationContext(url=site_url)
        context_auth.acquire_token_for_app(client_id=client_id, client_secret=client_secret)
        ctx = ClientContext(site_url, context_auth)
        sp_list = ctx.web.lists.get_by_title(list_name)
        items = sp_list.get_items()
        ctx.load(items)
        ctx.execute_query()
        #data = [{field: item.properties[field] for field in view_fields} for item in items]
        return pd.DataFrame([item.properties for item in items])
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Example usage:
df = read_sharepoint_list("<your_site_url>", "<your_client_id>", "<your_client_secret>", "<your_list_name>")
if df is not None:
    print(df.head())
