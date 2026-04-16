import * as k8s from "@pulumi/kubernetes";
import * as fs from "fs";
import * as path from "path";
import { k8sProvider } from "./provider";

// Read the seccomp profile from the checked-in JSON file
const profileJson = fs.readFileSync(
  path.join(__dirname, "seccomp", "io-uring-allowed.json"),
  "utf-8",
);

// ConfigMap holding the profile — source of truth is the JSON file
const configMap = new k8s.core.v1.ConfigMap("seccomp-profile-iouring", {
  metadata: {
    name: "seccomp-profile-iouring",
    namespace: "benchmarking",
  },
  data: {
    "io-uring-allowed.json": profileJson,
  },
}, { provider: k8sProvider });

// DaemonSet that copies the profile to every node's kubelet seccomp dir
export const seccompInstaller = new k8s.apps.v1.DaemonSet("seccomp-installer", {
  metadata: {
    name: "seccomp-installer",
    namespace: "benchmarking",
    labels: { app: "seccomp-installer" },
  },
  spec: {
    selector: { matchLabels: { app: "seccomp-installer" } },
    template: {
      metadata: { labels: { app: "seccomp-installer" } },
      spec: {
        initContainers: [{
          name: "install",
          image: "busybox:1.37",
          command: ["sh", "-c", "mkdir -p /host-seccomp/profiles && cp /profiles/* /host-seccomp/profiles/"],
          volumeMounts: [
            { name: "profiles", mountPath: "/profiles", readOnly: true },
            { name: "host-seccomp", mountPath: "/host-seccomp" },
          ],
        }],
        containers: [{
          name: "pause",
          image: "registry.k8s.io/pause:3.10",
          resources: { requests: { cpu: "1m", memory: "4Mi" }, limits: { cpu: "1m", memory: "4Mi" } },
        }],
        volumes: [
          { name: "profiles", configMap: { name: "seccomp-profile-iouring" } },
          { name: "host-seccomp", hostPath: { path: "/var/lib/kubelet/seccomp", type: "DirectoryOrCreate" } },
        ],
        tolerations: [{ operator: "Exists" }],
      },
    },
  },
}, { provider: k8sProvider, dependsOn: [configMap] });
