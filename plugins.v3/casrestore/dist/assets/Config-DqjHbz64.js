import { c as __mf_1, p as __mf_16, e as __mf_12, f as __mf_15, _ as __mf_13, a as __mf_8, l as __mf_11, m as __mf_17, h as __mf_10, g as __mf_9, q as __mf_2, i as __mf_3, k as __mf_7, j as __mf_4, n as __mf_14 } from './_virtual_mf___mfe_internal__CasRestore__mf_owner__258351331282643__loadShare__vue__loadShare__-De8H7wIV.js';

const _hoisted_1 = { class: "pa-4" };
const _hoisted_2 = {
  class: "d-flex align-center",
  style: {"gap":"8px"}
};
const _hoisted_3 = {
  class: "d-flex align-center flex-wrap mb-3",
  style: {"gap":"4px"}
};
const _hoisted_4 = {
  key: 0,
  class: "text-body-2 text-medium-emphasis"
};


const _sfc_main = {
  __name: 'Config',
  props: {
  initialConfig: { type: Object, default: () => ({}) },
  api: { type: Object, default: () => ({}) },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' },
  nativeSubscribe: { type: Function, default: null },
},
  emits: ['save', 'close', 'switch'],
  setup(__props, { emit: __emit }) {

const props = __props;

const emit = __emit;

const cfg = __mf_1({
  enabled: false,
  watch_dir: '',
  account_id: '',
  scope: 'family',
  family_id: '',
  target_folder_id: '',
  target_folder_name: '',
  notify: true,
  onlyonce: false,
  overwrite: '0',
  ...(props.initialConfig || {}),
});

const accounts = __mf_1([]);
const families = __mf_1([]);
const accountHint = __mf_1('');
const familyHint = __mf_1('');
const loading = __mf_1(false);
const saving = __mf_1(false);
const clearing = __mf_1(false);
const saveMessage = __mf_1('');
const saveError = __mf_1(false);

// 目录级联选择（弹窗内）：folderStack 为面包屑（首项恒为根目录），folderList 为当前目录子文件夹
const folderDialog = __mf_1(false);
const folderStack = __mf_1([{ id: '-11', name: '根目录' }]);
const folderList = __mf_1([]);
const folderSelected = __mf_1(null);
const folderLoading = __mf_1(false);
const folderHint = __mf_1('');
// 暂存选择（确定才写回 cfg，取消不动已保存的目录）
const pendingFolderId = __mf_1('');
const pendingFolderName = __mf_1('');

function openFolderDialog() {
  pendingFolderId.value = cfg.value.target_folder_id;
  pendingFolderName.value = cfg.value.target_folder_name;
  folderStack.value = [{ id: '-11', name: '根目录' }];
  folderSelected.value = null;
  folderDialog.value = true;
  loadFolders('-11');
}

function confirmFolder() {
  cfg.value.target_folder_id = pendingFolderId.value;
  cfg.value.target_folder_name = pendingFolderName.value;
  folderDialog.value = false;
}

async function clearRun() {
  clearing.value = true;
  try {
    await props.api.post(`plugin/${props.pluginId}/clear_run`);
  } catch (e) {
    console.error('CasRestore clear_run failed', e);
  } finally {
    clearing.value = false;
  }
}

async function loadAccounts() {
  try {
    const r = await props.api.get(`plugin/${props.pluginId}/accounts`);
    if (r && r.success) {
      accounts.value = r.items || [];
      accountHint.value = `共 ${accounts.value.length} 个账号`;
      if (!cfg.value.account_id && accounts.value.length) {
        cfg.value.account_id = accounts.value[0].value;
      }
    } else {
      accounts.value = [];
      accountHint.value = (r && r.message) || '拉取失败';
    }
  } catch (e) {
    accountHint.value = '拉取失败：' + (e && e.message ? e.message : e);
  }
}

async function loadFamilies() {
  if (!cfg.value.account_id || cfg.value.scope !== 'family') {
    families.value = [];
    familyHint.value = cfg.value.scope !== 'family' ? '个人空间无需选择家庭' : '先选择账号';
    return
  }
  try {
    const r = await props.api.get(`plugin/${props.pluginId}/families`, {
      params: { account_id: cfg.value.account_id },
    });
    if (r && r.success) {
      families.value = r.items || [];
      familyHint.value = `共 ${families.value.length} 个家庭`;
    } else {
      families.value = [];
      familyHint.value = (r && r.message) || '拉取失败';
    }
  } catch (e) {
    families.value = [];
    familyHint.value = '拉取失败：' + (e && e.message ? e.message : e);
  }
}

async function loadFolders(folderId, name) {
  if (!cfg.value.account_id) {
    folderList.value = [];
    folderHint.value = '先选择账号';
    return
  }
  if (cfg.value.scope === 'family' && !cfg.value.family_id) {
    folderList.value = [];
    folderHint.value = '先选择家庭';
    return
  }
  folderLoading.value = true;
  try {
    const r = await props.api.get(`plugin/${props.pluginId}/folders`, {
      params: {
        account_id: cfg.value.account_id,
        scope: cfg.value.scope,
        family_id: cfg.value.family_id,
        folder_id: folderId,
      },
    });
    if (r && r.success) {
      folderList.value = r.items || [];
      folderHint.value = folderList.value.length ? '选择一个子文件夹进入' : '（当前目录无子文件夹）';
    } else {
      folderList.value = [];
      folderHint.value = (r && r.message) || '拉取失败';
    }
  } catch (e) {
    folderList.value = [];
    folderHint.value = '拉取失败：' + (e && e.message ? e.message : e);
  } finally {
    folderLoading.value = false;
  }
}

function onFolderPick(item) {
  folderSelected.value = null;
  if (!item || !item.value) return
  folderStack.value.push({ id: item.value, name: item.name || item.label });
  pendingFolderId.value = item.value;
  pendingFolderName.value = item.name || item.label;
  loadFolders(item.value, item.name || item.label);
}

function goToFolder(index) {
  folderStack.value = folderStack.value.slice(0, index + 1);
  const top = folderStack.value[folderStack.value.length - 1];
  pendingFolderId.value = top.id === '-11' ? '' : top.id;
  pendingFolderName.value = top.id === '-11' ? '' : top.name;
  loadFolders(top.id, top.name);
}

async function save() {
  if (saving.value) return
  saving.value = true;
  saveError.value = false;
  saveMessage.value = '';
  try {
    const payload = { ...cfg.value };
    try {
      if (typeof props.api.put === 'function') {
        await props.api.put(`plugin/${props.pluginId}`, payload);
      } else {
        await props.api.post(`plugin/${props.pluginId}/save_config`, { config: payload });
      }
      saveMessage.value = '配置已保存';
      emit('save', payload);
    } catch (e) {
      console.error('CasRestore save config failed', e);
      saveError.value = true;
      saveMessage.value = '保存失败：' + (e && (e.message || (e.response && e.response.data && e.response.data.detail)) || e);
    }
  } finally {
    setTimeout(() => {
      saving.value = false;
    }, 500);
  }
}

__mf_16(() => cfg.value.account_id, loadFamilies);
__mf_16(() => cfg.value.scope, loadFamilies);
__mf_16(() => [cfg.value.account_id, cfg.value.scope, cfg.value.family_id], () => {
  // 账号/家庭变了，若目录弹窗开着则重新从根目录加载；不清空已保存的目录选择
  if (folderDialog.value) {
    folderStack.value = [{ id: '-11', name: '根目录' }];
    pendingFolderId.value = '';
    pendingFolderName.value = '';
    loadFolders('-11');
  }
});

__mf_12(async () => {
  loading.value = true;
  try {
    // 宿主未传 initialConfig 时（如从插件详情 Page 入口进入），从后端拉取已保存配置
    if (!props.initialConfig || !Object.keys(props.initialConfig).length) {
      try {
        const r = await props.api.get(`plugin/${props.pluginId}/config`);
        if (r && r.success && r.config) {
          Object.assign(cfg.value, r.config);
        }
      } catch (e) {
        console.error('CasRestore load config failed', e);
      }
    }
    await loadAccounts();
    await loadFamilies();
  } finally {
    loading.value = false;
  }
});

return (_ctx, _cache) => {
  const _component_v_card_title = __mf_15("v-card-title");
  const _component_v_alert = __mf_15("v-alert");
  const _component_v_switch = __mf_15("v-switch");
  const _component_v_col = __mf_15("v-col");
  const _component_v_btn = __mf_15("v-btn");
  const _component_v_select = __mf_15("v-select");
  const _component_v_text_field = __mf_15("v-text-field");
  const _component_v_row = __mf_15("v-row");
  const _component_v_card_text = __mf_15("v-card-text");
  const _component_v_spacer = __mf_15("v-spacer");
  const _component_v_card_actions = __mf_15("v-card-actions");
  const _component_v_card = __mf_15("v-card");
  const _component_v_chip = __mf_15("v-chip");
  const _component_v_dialog = __mf_15("v-dialog");

  return (__mf_13(), __mf_8("div", _hoisted_1, [
    __mf_11(_component_v_card, null, {
      default: __mf_17(() => [
        __mf_11(_component_v_card_title, null, {
          default: __mf_17(() => [...(_cache[11] || (_cache[11] = [
            __mf_10("CAS 恢复源文件 · 设置", -1)
          ]))]),
          _: 1
        }),
        __mf_11(_component_v_card_text, null, {
          default: __mf_17(() => [
            __mf_11(_component_v_alert, {
              type: "info",
              variant: "tonal",
              density: "compact",
              class: "mb-4"
            }, {
              default: __mf_17(() => [...(_cache[12] || (_cache[12] = [
                __mf_10(" 监控目录有新 .cas 写入时自动恢复；CAS 服务地址与 API Key 读取「CAS助手」插件，账号 / 家庭 / 目标路径在此选择。 ", -1)
              ]))]),
              _: 1
            }),
            __mf_11(_component_v_row, null, {
              default: __mf_17(() => [
                __mf_11(_component_v_col, {
                  cols: "12",
                  md: "3"
                }, {
                  default: __mf_17(() => [
                    __mf_11(_component_v_switch, {
                      modelValue: cfg.value.enabled,
                      "onUpdate:modelValue": _cache[0] || (_cache[0] = $event => ((cfg.value.enabled) = $event)),
                      label: "启用插件",
                      density: "compact",
                      "hide-details": ""
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                }),
                __mf_11(_component_v_col, {
                  cols: "12",
                  md: "3"
                }, {
                  default: __mf_17(() => [
                    __mf_11(_component_v_switch, {
                      modelValue: cfg.value.notify,
                      "onUpdate:modelValue": _cache[1] || (_cache[1] = $event => ((cfg.value.notify) = $event)),
                      label: "发送通知",
                      density: "compact",
                      "hide-details": ""
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                }),
                __mf_11(_component_v_col, {
                  cols: "12",
                  md: "3"
                }, {
                  default: __mf_17(() => [
                    __mf_11(_component_v_switch, {
                      modelValue: cfg.value.onlyonce,
                      "onUpdate:modelValue": _cache[2] || (_cache[2] = $event => ((cfg.value.onlyonce) = $event)),
                      label: "立即运行一次",
                      density: "compact",
                      "hide-details": ""
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                }),
                __mf_11(_component_v_col, {
                  cols: "12",
                  md: "3",
                  class: "d-flex align-center"
                }, {
                  default: __mf_17(() => [
                    __mf_11(_component_v_btn, {
                      color: "warning",
                      variant: "outlined",
                      block: "",
                      loading: clearing.value,
                      onClick: clearRun
                    }, {
                      default: __mf_17(() => [...(_cache[13] || (_cache[13] = [
                        __mf_10(" 清除运行记录 ", -1)
                      ]))]),
                      _: 1
                    }, 8, ["loading"])
                  ]),
                  _: 1
                }),
                __mf_11(_component_v_col, {
                  cols: "12",
                  md: "4"
                }, {
                  default: __mf_17(() => [
                    __mf_11(_component_v_select, {
                      modelValue: cfg.value.account_id,
                      "onUpdate:modelValue": _cache[3] || (_cache[3] = $event => ((cfg.value.account_id) = $event)),
                      items: accounts.value,
                      label: "恢复账号",
                      hint: accountHint.value,
                      "persistent-hint": "",
                      density: "compact",
                      "item-title": "label",
                      "item-value": "value"
                    }, null, 8, ["modelValue", "items", "hint"])
                  ]),
                  _: 1
                }),
                __mf_11(_component_v_col, {
                  cols: "12",
                  md: "4"
                }, {
                  default: __mf_17(() => [
                    __mf_11(_component_v_select, {
                      modelValue: cfg.value.scope,
                      "onUpdate:modelValue": _cache[4] || (_cache[4] = $event => ((cfg.value.scope) = $event)),
                      items: [
                { title: '个人空间', value: 'personal' },
                { title: '家庭空间', value: 'family' },
              ],
                      label: "恢复位置",
                      "item-title": "title",
                      "item-value": "value",
                      density: "compact",
                      "hide-details": ""
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                }),
                __mf_11(_component_v_col, {
                  cols: "12",
                  md: "4"
                }, {
                  default: __mf_17(() => [
                    __mf_11(_component_v_select, {
                      modelValue: cfg.value.family_id,
                      "onUpdate:modelValue": _cache[5] || (_cache[5] = $event => ((cfg.value.family_id) = $event)),
                      items: families.value,
                      label: "家庭列表",
                      hint: familyHint.value,
                      "persistent-hint": "",
                      density: "compact",
                      "item-title": "label",
                      "item-value": "value"
                    }, null, 8, ["modelValue", "items", "hint"])
                  ]),
                  _: 1
                }),
                __mf_11(_component_v_col, { cols: "12" }, {
                  default: __mf_17(() => [
                    __mf_11(_component_v_text_field, {
                      modelValue: cfg.value.watch_dir,
                      "onUpdate:modelValue": _cache[6] || (_cache[6] = $event => ((cfg.value.watch_dir) = $event)),
                      label: "监控目录",
                      hint: "容器内绝对路径（含子目录）",
                      "persistent-hint": "",
                      density: "compact"
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                }),
                __mf_11(_component_v_col, { cols: "12" }, {
                  default: __mf_17(() => [
                    __mf_9("div", _hoisted_2, [
                      __mf_11(_component_v_text_field, {
                        "model-value": cfg.value.target_folder_name || '',
                        label: "目标文件夹",
                        hint: cfg.value.target_folder_id ? `已选择目录 ID：${cfg.value.target_folder_id}` : '未选择，点击右侧按钮选择',
                        "persistent-hint": "",
                        density: "compact",
                        readonly: "",
                        class: "flex-grow-1"
                      }, null, 8, ["model-value", "hint"]),
                      __mf_11(_component_v_btn, {
                        color: "primary",
                        variant: "outlined",
                        onClick: openFolderDialog
                      }, {
                        default: __mf_17(() => [...(_cache[14] || (_cache[14] = [
                          __mf_10(" 选择目录 ", -1)
                        ]))]),
                        _: 1
                      })
                    ])
                  ]),
                  _: 1
                }),
                __mf_11(_component_v_col, { cols: "12" }, {
                  default: __mf_17(() => [
                    __mf_11(_component_v_select, {
                      modelValue: cfg.value.overwrite,
                      "onUpdate:modelValue": _cache[7] || (_cache[7] = $event => ((cfg.value.overwrite) = $event)),
                      items: [
                { title: '跳过（网盘已有同名文件时不恢复）', value: '0' },
                { title: '覆盖（网盘已有同名文件时覆盖）', value: '1' },
              ],
                      label: "同名文件",
                      hint: "恢复时若网盘已存在同名文件，选择跳过还是覆盖",
                      "persistent-hint": "",
                      density: "compact",
                      "item-title": "title",
                      "item-value": "value"
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                })
              ]),
              _: 1
            })
          ]),
          _: 1
        }),
        __mf_11(_component_v_card_actions, null, {
          default: __mf_17(() => [
            (saveMessage.value)
              ? (__mf_13(), __mf_8("span", {
                  key: 0,
                  class: __mf_2(["text-body-2", saveError.value ? 'text-error' : 'text-success'])
                }, __mf_3(saveMessage.value), 3))
              : __mf_7("", true),
            __mf_11(_component_v_spacer),
            __mf_11(_component_v_btn, {
              color: "primary",
              loading: saving.value,
              onClick: save
            }, {
              default: __mf_17(() => [...(_cache[15] || (_cache[15] = [
                __mf_10("保存配置", -1)
              ]))]),
              _: 1
            }, 8, ["loading"])
          ]),
          _: 1
        })
      ]),
      _: 1
    }),
    __mf_11(_component_v_dialog, {
      modelValue: folderDialog.value,
      "onUpdate:modelValue": _cache[10] || (_cache[10] = $event => ((folderDialog).value = $event)),
      "max-width": "640"
    }, {
      default: __mf_17(() => [
        __mf_11(_component_v_card, null, {
          default: __mf_17(() => [
            __mf_11(_component_v_card_title, null, {
              default: __mf_17(() => [...(_cache[16] || (_cache[16] = [
                __mf_10("选择目标文件夹", -1)
              ]))]),
              _: 1
            }),
            __mf_11(_component_v_card_text, null, {
              default: __mf_17(() => [
                __mf_9("div", _hoisted_3, [
                  (__mf_13(true), __mf_8(__mf_4, null, __mf_14(folderStack.value, (f, i) => {
                    return (__mf_13(), __mf_8(__mf_4, {
                      key: f.id
                    }, [
                      (i > 0)
                        ? (__mf_13(), __mf_8("span", _hoisted_4, "/"))
                        : __mf_7("", true),
                      __mf_11(_component_v_chip, {
                        size: "x-small",
                        color: i === folderStack.value.length - 1 ? 'primary' : 'default',
                        variant: "tonal",
                        onClick: $event => (goToFolder(i))
                      }, {
                        default: __mf_17(() => [
                          __mf_10(__mf_3(f.name), 1)
                        ]),
                        _: 2
                      }, 1032, ["color", "onClick"])
                    ], 64))
                  }), 128))
                ]),
                __mf_11(_component_v_select, {
                  modelValue: folderSelected.value,
                  "onUpdate:modelValue": [
                    _cache[8] || (_cache[8] = $event => ((folderSelected).value = $event)),
                    onFolderPick
                  ],
                  items: folderList.value,
                  loading: folderLoading.value,
                  label: "子文件夹",
                  hint: folderHint.value,
                  "persistent-hint": "",
                  density: "compact",
                  "item-title": "label",
                  "return-object": ""
                }, null, 8, ["modelValue", "items", "loading", "hint"]),
                __mf_9("div", {
                  class: __mf_2(["text-body-2 mt-3", pendingFolderId.value ? 'text-success' : 'text-medium-emphasis'])
                }, " 当前选中：" + __mf_3(pendingFolderName.value || '（根目录，请选择一个文件夹）'), 3)
              ]),
              _: 1
            }),
            __mf_11(_component_v_card_actions, null, {
              default: __mf_17(() => [
                __mf_11(_component_v_spacer),
                __mf_11(_component_v_btn, {
                  variant: "text",
                  onClick: _cache[9] || (_cache[9] = $event => (folderDialog.value = false))
                }, {
                  default: __mf_17(() => [...(_cache[17] || (_cache[17] = [
                    __mf_10("取消", -1)
                  ]))]),
                  _: 1
                }),
                __mf_11(_component_v_btn, {
                  color: "primary",
                  onClick: confirmFolder
                }, {
                  default: __mf_17(() => [...(_cache[18] || (_cache[18] = [
                    __mf_10("确定", -1)
                  ]))]),
                  _: 1
                })
              ]),
              _: 1
            })
          ]),
          _: 1
        })
      ]),
      _: 1
    }, 8, ["modelValue"])
  ]))
}
}

};

export { _sfc_main as default };
