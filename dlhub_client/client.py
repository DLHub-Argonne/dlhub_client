import requests
import pandas as pd


class DLHub:
    """Main class for interacting with the DLHub service"""
    service = "https://dlhub.org/api/v1"

    def __init__(self):
        pass

    def get_servables(self):
        """Get a list of the servables available in the service

        Returns:
            (pd.DataFrame) Summary of all the models available in the service
        """
        r = requests.get("{service}/servables".format(service=self.service))
        return pd.DataFrame(r.json())

    def get_id_by_name(self, name):
        """Get the ID of a DLHub servable by name

        Args:
            name (string): Name of the servable
        Returns:
            (string) UUID of the servable
        """
        r = requests.get("{service}/servables".format(service=self.service))
        df_tmp = pd.DataFrame(r.json())
        serv = df_tmp[df_tmp.name == name]
        return serv.iloc[0]['uuid']

    def run(self, servable_id, data):
        """Invoke a DLHub servable

        Args:
            servable_id (string): UUID of the servable
            data (dict): Dictionary of the data to send to the servable
        Returns:
            (pd.DataFrame): Reply from the service
        """
        servable_path = '{service}/servables/{servable_id}/run'.format(service=self.service,
                                                                       servable_id=servable_id)

        r = requests.post(servable_path, json=data)
        if r.status_code is not 200:
            raise Exception(r)
        return pd.DataFrame(r.json())
