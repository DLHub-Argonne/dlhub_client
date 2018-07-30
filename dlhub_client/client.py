import requests
import pandas as pd
import ipywidgets
import mdf_toolbox # Delete comment after adding mdf_toolbox to requirements
from mdf_forge.forge import Query # Delete comment after adding mdf_toolbox to requirements

# Maximum number of results per search allowed by Globus Search
SEARCH_LIMIT = 10000

SEARCH_INDEX_UUIDS = {
    "dlhub": "847c9105-18a0-4ffb-8a71-03dd76dfcc9d",
    "dlhub-test": "5c89e0a9-00e5-4171-b415-814fe4d0b8af"
}

class DLHub():
    service = "http://dlhub.org:5000/api/v1"
    index = SEARCH_INDEX_UUIDS["dlhub-test"] # Change to dlhub after test stage

    def __init__(self):
        self.__search_client = mdf_toolbox.login(services=["search_ingest"])["search_ingest"]
        self.__query = Query(self.__search_client)

    def get_servables(self):
        r = requests.get("{service}/servables".format(service=self.service), timeout=10)
        return pd.DataFrame(r.json())

    def get_id_by_name(self, name):
        r = requests.get("{service}/servables".format(service=self.service), timeout=10)
        df_tmp =  pd.DataFrame(r.json())
        serv = df_tmp[df_tmp.name==name]
        return serv.iloc[0]['uuid']

    def run(self, servable_id, data):
        servable_path = '{service}/servables/{servable_id}/run'.format(service=self.service,
                                                                       servable_id=servable_id)
        payload = {"data":data}

        r = requests.post(servable_path, json=data)
        if r.status_code is not 200:
            raise Exception(r)
        return pd.DataFrame(r.json())

    def search(self, q=None, index=None, advanced=False, limit=SEARCH_LIMIT, info=False,
               reset_query=True):
        """Execute a search and return the results.
        Args:
            q (str): The query to execute. Defaults to the current query, if any.
                    There must be some query to execute.
            index (str): The Globus Search index to search on. Defaults to the current index.
            advanced (bool): If **True**, will submit query in "advanced" mode
                    to enable field matches.
                    If **False**, only basic fulltext term matches will be supported.
                    Default **False**.
                    This value will change to **True** automatically
                    if the query is built with helpers.
            limit (int): The maximum number of results to return.
                    The max for this argument is the SEARCH_LIMIT imposed by Globus Search.
            info (bool): If **False**, search will return a list of the results.
                    If **True**, search will return a tuple containing the results list
                    and other information about the query.
                    Default **False**.
            reset_query (bool): If **True**, will destroy the query after execution
                    and start a fresh one.
                    If **False**, keeps the current query alive.
                    Default **True**.
        Returns:
            list (if info=False): The results.
        Returns:
            tuple (if info=True): The results, and a dictionary of query information.
        """
        if not index:
            index = self.index
        res = self.__query.search(q=q, index=index, advanced=advanced, limit=limit, info=info)
        if reset_query:
            self.reset_query()
        return res

    def match_field(self, field, value, required=True, new_group=False):
        """Add a field:value term to the query.
        Matches will have field == value.
        Args:
            field (str): The field to check for the value.
                    The field must be namespaced according to Elasticsearch rules
                    using the dot syntax.
                    Ex. "dlhub.domain" is the "domain" field of the "dlhub" dictionary.
            value (str): The value to match.
            required (bool): If **True**, will add term with AND. If **False**, will use OR.
                    Default **True**.
            new_group (bool): If **True**, will separate term into new parenthetical group.
                    If **False**, will not.
                    Default **False**.
        Returns:
            self (DLHub): For chaining.
        """
        # If not the start of the query string, add an AND or OR
        if self.__query.initialized:
            if required:
                self.__query.and_join(new_group)
            else:
                self.__query.or_join(new_group)
        self.__query.field(str(field), str(value))
        return self

    def search_by_domain(self, domain, index=None, limit=None, info=False):
        """Discover models based on the domain of the work,
        Args:
            domain (str): The domain to match againstself.
            Ex. "image recognition"

        Returns:
            Query results from Globus Search
        """
        space = " "
        if not isinstance(domain, str):
            raise ValueError("The input domain must be a str")
        domain.strip(space) # Can't lead or end with space

        if not len(domain) > 0:
            raise ValueError("A domain must be specified.")

        #domain.replace(" ", "\ ")
        self.match_field(field="dlhub.domain", value=domain)
        return self.search(index=index, limit=limit, info=info)

    def search_by_titles(self, titles, index=None, limit=None, info=False):
        """Add titles to the query.
        Args:
            titles (str or list of str): The titles to match.
        Returns:
            self (DLHub): For chaining.
        """
        if not titles:
            raise ValueError("At least one title must be specified")

        if not isinstance(titles, list):
            titles = [titles]

        self.match_field(field="datacite.title", value=titles[0], required=True, new_group=True)
        for title in titles[1:]:
            self.match_field(field="datacite.title", value=title, required=False, new_group=False)

        return self.search(index=index, limit=limit, info=info)

    def reset_query(self):
        """Destroy the current query and create a fresh one.
        Returns:
            None: Does not return self because this method should not be chained.
        """
        del self.__query
        self.__query = Query(self.__search_client)

    def get_input_info(self, name, index=None, info=False, output=False):
        """
        Args:
            name (str): Source name of the models
            index (str): The Globus Search index to search on. Defaults to the current index.
            info (bool): If **True** a more verbose output is given
            output (bool): If **True** gives description of prediction data returned
        Returns:
            str (if info=False): the shape of the input (or output) to be predicted on
        Returns:
            str, str (if info=True): the shape of the input (or output)
            and the input (or output) description
        """
        hits = self.match_field(field="servable.name", value=name).search(index=index)

        if hits == []: # No model found
            raise ValueError("No such model found: {}".format(name))

        elif len(hits) > 1:
            raise ValueError("Unexpectedly matched multiple models with name '{}'. "
                             "Please contact DLHub support.".format(name))

        base = hits[0]["servable"]["run"]
        if not output:
            shape = base["input"]["shape"]
            shape_info = base["input"]["description"]
        else: # Info on output prediction
            shape = base["output"]["shape"]
            shape_info = base["output"]["description"]

        if info:
            return shape, shape_info
        else:
            return shape

    def get_output_info(self, name, index=None, info=False):
        """running get_output_info(name) is equivalent to running
        get_input_info(name, output=True)
        Args:
            name (str): source name of the models
            index (str): The Globus Search index to search on. Defaults to the current index.
            info (bool): If **True** a more verbose output is given
        Returns:
            str (if info=False): the shape of the output to be predicted on
        Returns:
            str, str (if info=True): the shape of the output and the output description
        """
        return self.get_input_info(name, index=index, info=info, output=True)
