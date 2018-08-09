# DLHub-Client

DLHub-Client is the python package for leveraging the DLHub service. DLHub Client allows users to easily access machine learning and deep learning models to facilitate scientific discovery.

## Installation
```
pip install dlhub_client
```

### For Developers
```
git clone https://github.com/DLHub-Argonne/dlhub_client.git
cd dlhub_client
pip install -e .
```

## Examples
```python
from dlhub_client import client

# Instantiate the client
dl = client.DLHub()

# Search for recently published models in the chemistry domain
hits = dl.match_years(years=[2018]).search_by_domain("chemistry")
# Use the first match
hit = hits[0]
```
After running the result will appear similar to below:
```json
{
  "datacite": {
    "associatedPubliations": [
      "None"
    ],
    "creators": [
      "Zhu, Mengyuan"
    ],
    "description": "Use deep learning to read SMILES sequences and predict ADMET properties.",
    "license": "https:\/\/opensource.org\/licenses\/MIT",
    "publicationYear": 2018,
    "publisher": "DLHub",
    "resourceType": "Dataset",
    "title": "Including crystal structure attributes in machine learning models of formation energies via Voronoi tessellations"
  },
  "dlhub": {
    "domain": "chemistry",
    "version": "0.1",
    "visible_to": "public"
  },
  "servable": {
    "language": "python",
    "location": "s3:\/\/dlhub-anl\/servables\/deep_smiles",
    "ml_model": "CNN",
    "model_type": "keras",
    "name": "deep_smiles",
    "run": {
      "handler": "application.run",
      "input": {
        "description": "Input path to numpy or smiles file",
        "shape": "(,)",
        "type": "numpy array"
      },
      "output": {
        "description": "Output is a numpy array of predicted ADMET properties (one for each SMILES string).",
        "shape": "(,)",
        "type": "probability"
      }
    },
    "type": "model"
  }
}
```
To Run a prediction on the model:
```python
# This model requires a list of smiles strings as an input
data = ["HC(H)=C(H)(H)"] # Ethene
# Format data for request
payload = {"data":data}
# Get the model id
deep_smiles_id = dl.get_id_by_name("deep_smiles")
# Run the prediction
prediction = dl.run(deep_smiles_id, payload)

# Print out the result
print("The prediction for Ethene ADMET properties is {}".format(prediction))
```
This specific example will return`[0.5837262272834778]` as a result


More examples can be found on the [Examples](https://github.com/DLHub-Argonne/dlhub_examples) page.
