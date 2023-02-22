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
      "os": "cos",
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
            key: "delete_vm",
            value: "TRUE"
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
              "networkTier": "PREMIUM"
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
  const data = base64data ? JSON.parse(Buffer.from(base64data, 'base64').toString()) : {};
  console.log(data);

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
  const machineType = data.machine_type || process.env.MACHINE_TYPE || 'n1-standard-1'; // the cheapest "f1-micro"

  const vmConfig = getVMConfig(projectId, dockerImageUrl, serviceAccount, machineType);

  let idx = vmConfig.metadata.items.findIndex(el => el.key === 'config_uri');
  if (idx > -1) {
    vmConfig.metadata.items.splice(idx, 1)
  }

  // Get a config uri (config.yaml for app.py) from the pub/sub message payload,
  // And if it exists pass it as a custom metadata key-value to VM
  const config_uri = data.config_uri;
  if (config_uri && config_uri.trim().length > 1) {
    vmConfig.metadata.items.push({
      "key": "config_uri",
      "value": config_uri.trim()
    });
  }

  const vmName = instanceName + '-' + Date.now();

  console.log(`Creating a VM '${vmName}' with config: ` + JSON.stringify(vmConfig))
  compute.zone(zone)
    .createVM(vmName, vmConfig)
    .then(data => {
      // Operation pending.
      const vm = data[0];
      const operation = data[1];
      console.log(`VM being created: ${vm.id}`);
      console.log(`Operation info: ${operation.id}`);
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
