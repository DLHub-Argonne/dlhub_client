from dlhub_toolbox.utils.schemas import validate_against_dlhub_schema
from tempfile import mkstemp
import pandas as pd
import requests
import uuid
import os


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

    def submit_servable(self, model):
        """Submit a servable to DLHub

        If this servable has not been published before, it will be assigned a unique identifier.

        If it has been published before (DLHub detects if it has an identifier), then DLHub
        will update the model to the new version.

        Args:
            model (BaseMetadataModel): Model to be submitted
        Returns:
            (string) Task ID of this submission, used for checking for success
        """

        # If unassigned, give the model a UUID
        if model.dlhub_id is None:
            model.set_dlhub_id(str(uuid.uuid1()))

        # Get the metadata
        metadata = model.to_dict(simplify_paths=True)

        # Mark the method used to submit the model
        metadata['dlhub']['transfer_method'] = {'POST': 'file'}

        # Validate against the servable schema
        validate_against_dlhub_schema(metadata, 'servable')

        # Get the data to be submitted as a ZIP file
        fp, zip_filename = mkstemp('.zip')
        os.close(fp)
        os.unlink(zip_filename)
        try:
            model.get_zip_file(zip_filename)

            # Submit data to DLHub service
            with open(zip_filename, 'rb') as zf:
                req = requests.post('{service}/publish'.format(service=self.service), json=metadata,
                                    files={'files': zf})

            # Return the task id
            return req.json()['task_id']
        finally:
            os.unlink(zip_filename)
