# hf-mirror

hf-mirror is a simple huggingface mirror file server.
It caches model or dataset file on local fs and oss bucket.

## Usage

### huggingface_hub SDK

If you want to download model files via official huggingface_hub sdk, first `HF_ENDPOINT` environment needs to be set to the mirror endpoint.  

```
export HF_ENDPOINT=http://127.0.0.1:8082/https://huggingface.co
```

```
from huggingface_hub import hf_hub_download
from huggingface_hub import snapshot_download

hf_hub_download("lysandre/arxiv-nlp", filename="flax_model.msgpack", local_dir=".", revision='main', resume_download=True)

hf_hub_download("glue", filename="dataset_infos.json", local_dir=".", revision='main', resume_download=True, repo_type='dataset')

snapshot_download(repo_id="lysandre/arxiv-nlp")
```

### curl

```
curl "http://127.0.0.1:8082/https://huggingface.co/lysandre/arxiv-nlp/resolve/main/config.json" -o config.json
```