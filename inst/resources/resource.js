var dsHPC = {
  settings: {
    "title": "dsHPC Execution Units",
    "description": "Select an administrator-managed execution unit. Connection credentials remain with the durable worker.",
    "web": "https://github.com/isglobal-brge/dsHPC",
    "categories": [
      { "name": "hpc", "title": "HPC / Schedulers" },
      { "name": "cloud", "title": "Cloud schedulers" }
    ],
    "types": [
      {
        "name": "dshpc-slurm",
        "title": "dsHPC - Slurm",
        "description": "An administrator-managed Slurm execution unit.",
        "tags": ["hpc"],
        "parameters": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [{
            "key": "unit_id", "type": "string", "title": "Unit ID",
            "description": "ID from the server dsHPC unit catalogue (for example slurm-prod).",
            "pattern": "^[a-z][a-z0-9._-]{0,63}$"
          }],
          "required": ["unit_id"]
        }
      },
      {
        "name": "dshpc-external",
        "title": "dsHPC - External gateway",
        "description": "An administrator-managed external HPC gateway or SSH wrapper.",
        "tags": ["hpc"],
        "parameters": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [{
            "key": "unit_id", "type": "string", "title": "Unit ID",
            "description": "ID from the server dsHPC unit catalogue (for example cluster-a).",
            "pattern": "^[a-z][a-z0-9._-]{0,63}$"
          }],
          "required": ["unit_id"]
        }
      },
      {
        "name": "dshpc-kubernetes",
        "title": "dsHPC - Kubernetes",
        "description": "An administrator-managed Kubernetes execution unit.",
        "tags": ["cloud"],
        "parameters": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [{
            "key": "unit_id", "type": "string", "title": "Unit ID",
            "description": "ID from the server dsHPC unit catalogue (for example k8s-gpu).",
            "pattern": "^[a-z][a-z0-9._-]{0,63}$"
          }],
          "required": ["unit_id"]
        }
      }
    ]
  },
  asResource: function(type, name, params, credentials) {
    var unitTypes = {
      "dshpc-slurm": "slurm",
      "dshpc-external": "external",
      "dshpc-kubernetes": "kubernetes"
    };
    if (!unitTypes[type]) return undefined;
    return {
      name: name,
      url: "dshpc+unit://" + unitTypes[type] + "/" + params.unit_id
    };
  }
};
