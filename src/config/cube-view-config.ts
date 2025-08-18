import path from 'node:path';
import { readFileSync } from 'fs';
import { CubeViewConfig } from '../interfaces/cube-view-config';

const configFile = path.join(__dirname, 'cube-view.json');
let loadedCubeConfig: CubeViewConfig[] | undefined;

function loadCubeConfigFromFile(): CubeViewConfig[] {
  loadedCubeConfig = JSON.parse(readFileSync(configFile).toString()) as CubeViewConfig[];
  return loadedCubeConfig;
}

export const cubeConfig = loadedCubeConfig ? loadedCubeConfig : loadCubeConfigFromFile();
