import { controllerStatefulSet } from "./controller";
import { seccompInstaller } from "./seccomp";

export const controller = controllerStatefulSet.metadata.name;
export const seccomp = seccompInstaller.metadata.name;
