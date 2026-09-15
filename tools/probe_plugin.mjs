/**
 * Exercise the KVFlow DSH plugin against a stub host.
 *
 * A stub proves the two things that are the plugin's own responsibility: it
 * registers the documented tool surface and one slash command, and every tool
 * call goes through the real KVFlow CLI bridge. It deliberately does NOT claim
 * that DSH itself loaded it -- that is what the composed profile tree and the
 * headless boot are for.
 */

import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';

const PRODUCT = 'C:/Users/90428/Desktop/KVStock-restored/kvflow';
const HOME = `${PRODUCT}/.runtime/smoke/home`;
const DEMO = `${PRODUCT}/.runtime/smoke/demo-python-api`;
const PYTHON = 'C:/Users/90428/Desktop/KVStock-restored/kvstock-platform/agent_os/releases/1.0.0-rc1/.venv/Scripts/python.exe';

const tools = new Map();
const commands = new Map();
const sections = [];

// Cordis hands a declared service in as a property and resolves it through
// ctx.get(); the stub does both so the plugin's real access pattern is exercised.
const services = {
  tools: { register: (spec) => tools.set(spec.name, spec) },
  commands: { register: (command) => commands.set(command.name, command) },
  systemPrompt: { section: (section) => sections.push(section) },
};

const ctx = {
  tools: services.tools,
  commands: services.commands,
  get: (name) => services[name],
  logger: { info: (message) => console.log('[kvflow]', message) },
};

const module = await import(pathToFileURL(resolve(PRODUCT, 'dsh-plugin/lib/index.js')).href);
module.apply(ctx, {
  home: HOME,
  python: PYTHON,
  pythonPath: `${PRODUCT}/src`,
  workspace: DEMO,
});

console.log('plugin name      ', module.name);
console.log('tools registered ', tools.size, [...tools.keys()].join(', '));
console.log('commands         ', [...commands.keys()].join(', '));
console.log('prompt sections  ', sections.map((section) => section.name).join(', '));

const current = tools.get('kvflow_current_project');
const bound = await current.execute({ path: DEMO });
console.log('binding          ', JSON.stringify(bound).slice(0, 220));

const projects = await tools.get('kvflow_projects').execute({});
console.log('projects         ', JSON.stringify({ count: projects.count, first: projects.projects[0]?.project_id }));

const templates = await tools.get('kvflow_templates').execute({});
console.log('templates        ', templates.templates.map((item) => item.id).join(', '));

const flow = commands.get('flow');
const unregistered = await flow.handler({ input: 'add a helper' });
console.log('flow unbound     ', unregistered.kind, String(unregistered.text).slice(0, 160).replace(/\n/g, ' | '));

const noInput = await flow.handler({ input: '' });
console.log('flow empty input ', noInput.kind, noInput.text);
