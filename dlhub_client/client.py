import requests
import pandas as pd
import ipywidgets
import mdf_toolbox
from mdf_forge.forge import Query

# Maximum number of results per search allowed by Globus Search
SEARCH_LIMIT = 10000

SEARCH_INDEX_UUIDS = {
    "dlhub": "847c9105-18a0-4ffb-8a71-03dd76dfcc9d",
    "dlhub-test": "5c89e0a9-00e5-4171-b415-814fe4d0b8af"
}

class DLHub():
    service = "http://dlhub.org:5000/api/v1"
    __default_index = "dlhub-test" # Change to dlhub after test stage

    def __init__(self, index=__default_index):
        self.index = index
        self.__search_client = mdf_toolbox.login(services=["search_ingest"])["search_ingest"]
        self.__query = Query(self.__search_client)

    @property
    def search_client(self):
        return self.__search_client

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
            domain (list or str): The domain(s) to match against
            Ex. "image recognition"

        Returns:
            Query results from Globus Search
        """
        # NOTE: Remove below after testing
        """space = " "
        if not isinstance(domain, str):
            raise ValueError("The input domain must be a str")
        domain.strip(space) # Can't lead or end with space

        if not len(domain) > 0:
            raise ValueError("A domain must be specified.")

        #domain.replace(" ", "\ ")
        self.match_field(field="dlhub.domain", value=domain)
        return self.search(index=index, limit=limit, info=info)"""
        if isinstance(domain, str):
            domain = [domain]
        if not isinstance(domain, list):
            raise ValueError("The input domain must be a list or str")

        # First source should be in new group and required
        self.match_field(field="dlhub.domain", value=domain[0], required=True, new_group=True)
        # Other sources should stay in that group, and not be required
        for d in domain[1:]:
            self.match_field(field="dlhub.domain", value=d, required=False, new_group=False)

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

    def match_years(self, years=None, start=None, stop=None, inclusive=True):
        """Add years and limits to the query.
        Args:
            years   (int or string, or list of int or strings): The years to match.
                    Note that this argument overrides the start, stop, and inclusive arguments.
            start   (int or string): The lower range of years to match.
            stop    (int or string): The upper range of years to match.
            inclusive (bool): If **True**, the start and stop values will be included in the search.
                    If **False**, they will be excluded.
                    Default **True**.
        Returns:
            self (DLHub): For chaining.
        """
        # If nothing supplied, nothing to match
        if years is None and start is None and stop is None:
            return self

        if years is not None and years != []:
            if not isinstance(years, list):
                years = [years]
            years_int = []
            for year in years:
                try:
                    y_int = int(year)
                    years_int.append(y_int)
                except ValueError:
                    print("Invalid year: '{}'".format(year))
                    #print("Invalid year: '", year, "'", sep="")

            # Only match years if valid years were supplied
            if len(years_int) > 0:
                self.match_field(field="datacite.publicationYear", value=years_int[0], required=True,
                                 new_group=True)
                for year in years_int[1:]:
                    self.match_field(field="datacite.publicationYear",
                                     value=year, required=False, new_group=False)
        else:
            if start is not None:
                try:
                    start = int(start)
                except ValueError:
                    print("Invalid start year: '", start, "'", sep="")
                    start = None
            if stop is not None:
                try:
                    stop = int(stop)
                except ValueError:
                    print("Invalid stop year: '", stop, "'", sep="")
                    stop = None

            self.match_range(field="datacite.publicationYear", start=start, stop=stop,
                             inclusive=inclusive, required=True, new_group=True)
        return self

    def match_range(self, field, start="*", stop="*", inclusive=True,
                    required=True, new_group=False):
        """Add a field:[some range] term to the query.
        Matches will have field == value in range.
        Args:
            field (str): The field to check for the value.
                    The field must be namespaced according to Elasticsearch rules using
                    the dot syntax.
                    Ex. "dlhub.domain" is the "domain" field of the "dlhub" dictionary.
            start (str or int): The starting value. "*" is acceptable to make no lower bound.
            stop (str or int): The ending value. "*" is acceptable to have no upper bound.
            inclusive (bool): If **True**, the start and stop values will be included
                    in the search.
                    If **False**, the start and stop values will not be included
                    in the search.
            required (bool): If **True**, will add term with AND. If **False**, will use OR.
                    Default **True**.
            new_group (bool): If **True**, will separate term into new parenthetical group.
                    If **False**, will not.
                    Default **False**.
        Returns:
            self (DLHub): For chaining.
        """
        # Accept None as *
        if start is None:
            start = "*"
        if stop is None:
            stop = "*"
        # No-op on *-*
        if start == "*" and stop == "*":
            return self

        if inclusive:
            value = "[" + str(start) + " TO " + str(stop) + "]"
        else:
            value = "{" + str(start) + " TO " + str(stop) + "}"
        self.match_field(field, value, required=required, new_group=new_group)
        return self
