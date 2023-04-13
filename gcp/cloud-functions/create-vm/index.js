const functions = require('@google-cloud/functions-framework');
const Compute = require('@google-cloud/compute');
const {ProjectsClient} = require('@google-cloud/resource-manager').v3;
const {GoogleAuth} = require('google-auth-library');

const compute = new Compute();

const instanceName = process.env.INSTANCE_NAME;
// NOTE: for Node10 and later all these EnvVars that were set automatically before aren't set anymore!
// Add environment variables manually on deployment
const region = process.env.REGION || 'us-central1';
const zone = process.env.ZONE || 'us-central1-a';

function getVMConfig(projectId, dockerImageUrl, serviceAccount, machineType)  {
  const vmConfig = {
    "kind": "compute#instance",
    "zone": `projects/${projectId}/zones/${zone}`,
    "machineType": `projects/${projectId}/zones/${zone}/machineTypes/${machineType}`,
    "os": "cos-stable",
    "displayDevice": {
      "enableDisplay": false
    },
    "metadata": {
      "kind": "compute#metadata",
      "items": [
        {
          key: "gce-container-declaration",
          value: `spec:\n  containers:\n    - name: ${instanceName}\n      image: "${dockerImageUrl}"\n      stdin: false\n      tty: false\n  restartPolicy: Never\n\n# This container declaration format is not public API and may change without notice. Please\n# use gcloud command-line tool or Google Cloud Console to run Containers on Google Compute Engine.`
        },
        {
          key: 'enable-oslogin',
          value: 'TRUE'
        },
        // enabling logging for container - https://cloud.google.com/container-optimized-os/docs/how-to/logging#gcloud
        {
          key: 'google-logging-enabled',
          value: 'TRUE'
        },
        {
          key: 'google-logging-use-fluentbit',
          value: 'TRUE'
        },
        {
          key: 'delete_vm',
          value: 'TRUE'
        }
      ]
    },
    "networkInterfaces": [
      {
        "kind": "compute#networkInterface",
        "subnetwork": `projects/${projectId}/regions/${region}/subnetworks/default`,
        "accessConfigs": [
          {
            "kind": "compute#accessConfig",
            "name": "External NAT",
            "type": "ONE_TO_ONE_NAT",
            "networkTier": "STANDARD" //"PREMIUM"
          }
        ]
      }
    ],
    "serviceAccounts": [
      {
        "email": serviceAccount,
        "scopes": [
          "https://www.googleapis.com/auth/cloud-platform"
        ]
      }
    ]
  };
  return vmConfig;
}

function setMetadata(items, key, value) {
  let idx = items.findIndex(el => el.key === key);
  if (idx > -1) {
    items.splice(idx, 1)
  }

  if (value && value.trim().length > 1) {
    items.push({
      "key": key,
      "value": value
    });
  }
}

async function getProject() {
  const auth = new GoogleAuth({
    scopes: 'https://www.googleapis.com/auth/cloud-platform'
  });
  const projectId = await auth.getProjectId();
  console.log(`detected project id: ${projectId}`);
  return projectId;
}


async function getDefaultServiceAccount(projectId) {
  const client = new ProjectsClient();
  const [project]= await client.getProject({
    name: 'projects/' + projectId
  });
  const projectNumber = project.name.substring("projects/".length);
  console.log(`Current project number: ${projectNumber}`);
  return `${projectNumber}-compute@developer.gserviceaccount.com`;
}


functions.cloudEvent('createInstance', async (cloudEvent) => {
  const base64data = cloudEvent.data.message.data;
  const data_str = base64data ? Buffer.from(base64data, 'base64').toString() : "";
  let data = {};
  try {
    data = data_str ? JSON.parse(data_str) : {};
  } catch(e) {
    console.error(e);
    throw new Error(`Failed to parse event data: ${data_str}\n${e.message}`);
  }
  console.log(JSON.stringify({event_data:data}));

  // get project id where create a VM, by default the current project is used
  let projectId = data.project_id || process.env.GCP_PROJECT
  if (!projectId) {
    projectId = await getProject();
  }

  // get a Docker image url, no default, should be specified in ENV or passed in runtime
  let dockerImageUrl = data.docker_image || process.env.DOCKER_IMAGE;
  if (!dockerImageUrl) {
    throw new Error('Docker image url was not specified');
  }

  // service account to run the VM under, by default the default Compute Engine service account is used
  let serviceAccount = data.service_account || process.env.SERVICE_ACCOUNT;
  if (!serviceAccount) {
    serviceAccount = await getDefaultServiceAccount(projectId);
  }
  console.log(`serviceAccount: ${serviceAccount}`)

  // initialize a machine type for VM to use
  const machineType = data.machine_type || process.env.MACHINE_TYPE || 'n1-standard-1'; // note: the cheapest is "f1-micro"

  const vmConfig = getVMConfig(projectId, dockerImageUrl, serviceAccount, machineType);

  // Get a config uri (config.yaml) and ads config uri (google-ads.yaml) from the pub/sub message payload,
  // And if it exists pass it as a custom metadata key-value to VM

  setMetadata(vmConfig.metadata.items, 'gcs_source_uri', data.gcs_source_uri);
  setMetadata(vmConfig.metadata.items, 'gcs_base_path_public', data.gcs_base_path_public);
  if (data.delete_vm !== undefined) {
    setMetadata(vmConfig.metadata.items, 'delete_vm', data.delete_vm);
  }

  // org policy can prevent using external IPs, if so we'll remove accessConfig and this will prevent assigning an external IP
  if (data.no_public_ip || process.env.NO_PUBLIC_IP) {
    vmConfig.networkInterfaces[0].accessConfigs = [];
  }

  const vmName = instanceName + '-' + Date.now();

  console.log(JSON.stringify({message: `Creating a VM '${vmName}' (see vm config in jsonPayload)`, vmConfig: vmConfig}));
  compute.zone(zone)
      .createVM(vmName, vmConfig)
      .then(data => {
        // Operation pending.
        const vm = data[0];
        const operation = data[1];
        console.log(`VM creation operation submitted (VM id: ${vm.id}, operation: ${operation.id})`);
        return operation.promise();
      })
      .then(() => {
        const message = 'VM created with success, Cloud Function finished execution.';
        console.log(message);
      })
      .catch(err => {
        console.error(err);
      });
});
