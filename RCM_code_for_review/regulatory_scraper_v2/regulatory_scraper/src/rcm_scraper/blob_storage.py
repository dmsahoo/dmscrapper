'''
Landing storage: the Azure Blob Storage container where the scraper saves every document.

Why Blob Storage and not ADLS: the storage account has no hierarchical namespace (HNS off),
because Microsoft Defender for Storage writes its malware scan result as a blob index tag,
and blob index tags are only available (GA) on accounts without HNS.

Path of each file (blob name):
  <prefix>/<regulator_id>/<first 16 characters of document_id>/v<version>/<file name>
Every version has its own folder, so nothing is ever overwritten.

Access uses a Unity Catalog service credential (managed identity). No keys.
'''

from rcm_scraper.common import sha256_bytes


class LandingStore:
    '''Writes and reads documents in the landing container.'''

    def __init__(self, container_client, prefix):
        self.container = container_client
        self.prefix = prefix.strip('/')

    def path_for(self, regulator_id, document_id, version, file_name):
        '''Blob name for one version of one document.'''
        return f'{self.prefix}/{regulator_id}/{document_id[:16]}/v{version}/{file_name}'

    def write_verified(self, blob_name, data, expected_hash):
        '''Upload, download again and confirm the fingerprint. Deletes the blob on a mismatch.'''
        self.container.upload_blob(blob_name, data, overwrite=True)
        written_hash = sha256_bytes(self.container.download_blob(blob_name).readall())
        if written_hash != expected_hash:
            self.remove_quietly(blob_name)
            raise IOError(f'Hash mismatch after uploading {blob_name}')
        return written_hash

    def remove_quietly(self, blob_name):
        '''Delete a blob, without failing the run if it cannot be deleted.'''
        try:
            self.container.delete_blob(blob_name)
            return True
        except Exception:
            return False

    def check_writable(self, run_id):
        '''Pre flight check: upload and delete a small test blob.'''
        probe = f'{self.prefix}/_preflight/{run_id}.txt'
        self.container.upload_blob(probe, b'ok', overwrite=True)
        self.container.delete_blob(probe)

    def read_tags(self, blob_name):
        '''All blob index tags of one blob, as a dictionary (empty if there are none).'''
        return self.container.get_blob_client(blob_name).get_blob_tags() or {}


def container_client(landing_config, credential):
    '''Blob container client from the landing block of the common config.'''
    from azure.storage.blob import BlobServiceClient
    url = f"https://{landing_config['storage_account']}.blob.core.windows.net"
    return BlobServiceClient(url, credential=credential).get_container_client(landing_config['container'])


def landing_store(landing_config, credential):
    '''LandingStore built from the landing block of the common config.'''
    return LandingStore(container_client(landing_config, credential), landing_config['prefix'])
