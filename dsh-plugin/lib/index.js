/**
 * KVFlow for DeepSeek Harness.
 *
 * This is a thin host-plane adapter, on purpose: it registers tools and one slash
 * command that call the KVFlow CLI, and the workflow itself runs in the Python
 * core, in the same durable store the CLI and the MCP server use. That is what
 * keeps "one authoritative task state" true instead of aspirational.
 *
 * What it does NOT do:
 *   * it does not plan, dispatch or review anything itself;
 *   * it does not widen any permission: it passes a requirement, a registered
 *     project id and a template id, and the approved project configuration decides
 *     scope, profiles and budget;
 *   * it does not block the host for the length of a run: `kvflow_start` returns a
 *     job id and the run continues in its own process (or in a background thread
 *     when the sandbox kills detached children, which is reported honestly).
 *
 * Configuration (see cordis.patch.yml):
 *   home       KVFlow runtime directory; default $KVFLOW_HOME or ~/.kvflow
 *   python     interpreter that can import kvflow; default $KVFLOW_PYTHON or python
 *   workspace  project directory to bind; default: resolved from the DSH workspace
 *   timeoutMs  per-CLI-call timeout; default 120000
 */

import { execFile } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { delimiter, join } from 'node:path';

export const name = 'kvflow';
//: Cordis refuses to hand a plugin a service it did not declare. Only the tool
//: registry is required: the command registry and the system-prompt section are
//: looked up optionally, so a host that has no slash commands (a headless profile)
//: still gets the tools instead of failing to load.
export const inject = ['tools'];
export const provide = [];

const PACKAGE_NAME = 'kvflow-dsh';
const DEFAULT_TIMEOUT_MS = 120_000;
const WORKSPACE_STORE = join(homedir(), '.dsh', 'storages', 'workspace.json');

function configOf(config) {
  return {
    home: config?.home || process.env.KVFLOW_HOME || join(homedir(), '.kvflow'),
    python: config?.python || process.env.KVFLOW_PYTHON || 'python',
    // where the kvflow package can be imported from, when it is not installed
    // into the interpreter itself (a source checkout, a bundled runtime)
    pythonPath: config?.pythonPath || process.env.KVFLOW_PYTHONPATH || null,
    workspace: config?.workspace || null,
    // A read-only pointer to the host's credential store. The desktop host does
    // not export DEEPSEEK_API_KEY to the processes it spawns, so without this the
    // KVFlow child has no provider credential and every workflow is refused with
    // AuthorityDenied. KVFlow reads the file itself; the value is a path.
    credentials: config?.credentials || process.env.KVFLOW_CREDENTIALS || null,
    timeoutMs: Number(config?.timeoutMs || DEFAULT_TIMEOUT_MS),
  };
}

/** The directory this host session is working in, if it can be determined. */
function currentWorkspace(config) {
  if (config.workspace) return config.workspace;
  try {
    if (existsSync(WORKSPACE_STORE)) {
      const store = JSON.parse(readFileSync(WORKSPACE_STORE, 'utf8'));
      const table = store?.tables?.workspaces || {};
      const paths = Object.values(table).map((entry) => entry?.path).filter(Boolean);
      if (paths.length === 1) return paths[0];
      const cwd = process.cwd();
      const inside = paths.find((path) => cwd === path || cwd.startsWith(path));
      if (inside) return inside;
      if (paths.length > 0) return paths[0];
    }
  } catch {
    /* a host storage that cannot be read is not a reason to fail a tool call */
  }
  return process.cwd();
}

/** Call one KVFlow tool through the CLI bridge: one implementation, no drift. */
function bridge(config, tool, args) {
  const settings = configOf(config);
  return new Promise((resolve) => {
    const argv = [
      '-m', 'kvflow.cli',
      '--home', String(settings.home),
      'bridge',
      '--tool', String(tool),
      '--args', JSON.stringify(args || {}),
    ];
    const env = { ...process.env };
    if (settings.pythonPath) {
      env.PYTHONPATH = [String(settings.pythonPath), env.PYTHONPATH || '']
        .filter(Boolean)
        .join(delimiter);
    }
    if (settings.credentials) {
      // the child reads this path itself; the pointer is never a secret and never
      // travels further than this one process
      env.KVFLOW_CREDENTIALS = String(settings.credentials);
    }
    env.PYTHONIOENCODING = 'utf-8';
    execFile(
      settings.python,
      argv,
      { timeout: settings.timeoutMs, windowsHide: true, maxBuffer: 8 * 1024 * 1024, env },
      (error, stdout, stderr) => {
        const text = String(stdout || '').trim();
        if (text) {
          try {
            resolve({ ok: true, value: JSON.parse(text) });
            return;
          } catch {
            resolve({ ok: false, value: { kind: 'error', text: text.slice(0, 4000) } });
            return;
          }
        }
        resolve({
          ok: false,
          value: {
            kind: 'error',
            text: String(stderr || error?.message || 'kvflow produced no output').slice(0, 4000),
          },
        });
      },
    );
  });
}

function renderJson(_args, value) {
  const body = value && value.ok === false && value.value ? value.value : value;
  // the host expects content blocks and calls .some() on them: a bare string is a
  // load-time-shaped mistake the host reports as "content.some is not a function"
  return [{ type: 'text', text: JSON.stringify(body, null, 2) }];
}

function registerTool(ctx, config, spec) {
  ctx.tools.register({
    name: spec.name,
    description: spec.description,
    parameters: spec.parameters,
    output: { schema: { type: 'object', additionalProperties: true }, render: renderJson },
    execute: async (args) => {
      const result = await bridge(config, spec.name, args);
      if (!result.ok) throw new Error(result.value?.text || 'kvflow call failed');
      return result.value;
    },
    presentCall: (args) => ({
      card: 'generic',
      title: spec.title,
      kind: 'search',
      rawInput: JSON.stringify(args || {}).slice(0, 400),
    }),
  });
}

const TOOL_SPECS = [
  {
    name: 'kvflow_projects',
    title: 'KVFlow projects',
    description: 'List the registered KVFlow projects and their registry health.',
    parameters: { type: 'object', properties: {} },
  },
  {
    name: 'kvflow_current_project',
    title: 'Bind the current project',
    description:
      'Resolve the current workspace directory to a registered KVFlow project, or report the exact onboarding command when it is not registered yet.',
    parameters: {
      type: 'object',
      properties: { path: { type: 'string', description: 'directory; defaults to the current workspace' } },
    },
  },
  {
    name: 'kvflow_project_status',
    title: 'Project scope and profiles',
    description:
      'The approved scope, execution profiles, model profile, budget and recent runs of one registered project.',
    parameters: {
      type: 'object',
      properties: { project_id: { type: 'string' } },
      required: ['project_id'],
    },
  },
  {
    name: 'kvflow_templates',
    title: 'Workflow templates',
    description: 'The workflow templates a registered project may use (feature, bugfix, refactor, docs_or_data).',
    parameters: { type: 'object', properties: {} },
  },
  {
    name: 'kvflow_start',
    title: 'Start a KVFlow workflow',
    description:
      'Start one development workflow on a registered project. Only the requirement, the project id and an optional template are accepted; every permission and budget comes from the project configuration the user approved.',
    parameters: {
      type: 'object',
      properties: {
        requirement: { type: 'string', description: 'what to build, fix, refactor or document' },
        project_id: { type: 'string', description: 'defaults to the current project binding' },
        template: { type: 'string', enum: ['feature', 'bugfix', 'refactor', 'docs_or_data'] },
      },
      required: ['requirement'],
    },
  },
  {
    name: 'kvflow_runs',
    title: 'Recent runs',
    description: 'Recent KVFlow runs, newest first, optionally for one project.',
    parameters: {
      type: 'object',
      properties: { project_id: { type: 'string' }, limit: { type: 'integer' } },
    },
  },
  {
    name: 'kvflow_status',
    title: 'Run status',
    description: 'Durable status of one run: state, node states, executor receipts.',
    parameters: { type: 'object', properties: { job_id: { type: 'string' } }, required: ['job_id'] },
  },
  {
    name: 'kvflow_result',
    title: 'Run result',
    description:
      'The evidence of a finished run: changed files, executor receipts, the manager review and the integration.',
    parameters: { type: 'object', properties: { job_id: { type: 'string' } }, required: ['job_id'] },
  },
  {
    name: 'kvflow_control',
    title: 'Pause, resume or cancel',
    description: 'Pause, resume or cancel one run. Cancel stops only that run\'s own process.',
    parameters: {
      type: 'object',
      properties: {
        job_id: { type: 'string' },
        action: { type: 'string', enum: ['pause', 'resume', 'cancel'] },
        reason: { type: 'string' },
      },
      required: ['job_id', 'action'],
    },
  },
  {
    name: 'kvflow_knowledge',
    title: 'Project knowledge',
    description: 'Search the knowledge visible to one project, with its provenance.',
    parameters: {
      type: 'object',
      properties: { project_id: { type: 'string' }, topic: { type: 'string' } },
      required: ['project_id'],
    },
  },
];

/** `/flow <task>`: start a workflow on the current project and return its id. */
function registerCommand(ctx, config) {
  const commands = ctx.get('commands', false);
  if (!commands || typeof commands.register !== 'function') return false;
  commands.register({
    name: 'flow',
    description: '用 KVFlow 在当前项目上执行一个开发任务（规划→派工→实现→测试→审核→集成）',
    input: { hint: '<任务描述> [--template feature|bugfix|refactor|docs_or_data]' },
    handler: async (invocation) => {
      const raw = String(invocation?.input || invocation?.args || '').trim();
      if (!raw) {
        return { kind: 'error', text: '用法：/flow <任务描述>' };
      }
      const match = raw.match(/--template\s+([a-z_]+)/i);
      const template = match ? match[1] : null;
      const requirement = raw.replace(/--template\s+[a-z_]+/i, '').trim();

      const binding = await bridge(config, 'kvflow_current_project', {
        path: currentWorkspace(configOf(config)),
      });
      const bound = binding.value?.bound === true ? binding.value.project : null;
      if (!bound) {
        return {
          kind: 'error',
          text:
            `当前目录还没有登记为 KVFlow 项目。\n${JSON.stringify(binding.value, null, 2)}\n` +
            '请先运行上面的 next 命令确认范围后再 /flow。',
        };
      }
      const started = await bridge(config, 'kvflow_start', {
        requirement,
        project_id: bound.project_id,
        ...(template ? { template } : {}),
      });
      if (!started.ok) {
        return { kind: 'error', text: started.value?.text || 'KVFlow 启动失败' };
      }
      const value = started.value;
      return {
        kind: 'text',
        text: [
          `KVFlow 已启动：${value.job_id}`,
          `项目：${value.project_id}（${bound.display_name}）`,
          `模板：${value.template}；模型档：${value.model_profile}；计划来源：${value.plan_source}`,
          `节点：${(value.nodes || []).join(', ')}`,
          `运行方式：${value.runner?.mode}`,
          '输入 kvflow_status / kvflow_result 查看进度与结果。',
        ].join('\n'),
      };
    },
  });
  return true;
}

function registerGuidance(ctx) {
  ctx.get('systemPrompt')?.section?.({
    name: 'kvflow:routing',
    order: 160,
    text: () =>
      'KVFlow tools start and follow real development workflows on registered projects.' +
      ' Use kvflow_current_project to bind the current workspace, then kvflow_start with' +
      ' the user\'s requirement. Do not plan the work yourself as well: KVFlow owns the' +
      ' plan, the dispatch, the review and the integration, and its durable state is the' +
      ' only authority on what a run has done.',
  });
}

export function apply(ctx, config) {
  for (const spec of TOOL_SPECS) registerTool(ctx, config, spec);
  // A host that has no command registry (a headless profile, for example) still
  // gets the tools; the slash command is an extra entrance, not a requirement.
  try {
    registerCommand(ctx, config);
  } catch (error) {
    ctx.logger?.warn?.(`${PACKAGE_NAME}: no command registry in this host (${error?.message})`);
  }
  try {
    registerGuidance(ctx);
  } catch (error) {
    ctx.logger?.warn?.(`${PACKAGE_NAME}: no system-prompt section in this host (${error?.message})`);
  }
  ctx.logger?.info?.(
    `${PACKAGE_NAME}: registered ${TOOL_SPECS.length} tools and /flow for ` +
      `runtime ${configOf(config).home}`,
  );
}
