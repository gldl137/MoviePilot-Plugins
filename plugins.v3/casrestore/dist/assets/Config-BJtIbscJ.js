import { c as __mf_1, n as __mf_13, d as __mf_9, e as __mf_12, _ as __mf_10, a as __mf_5, f as __mf_8, g as __mf_14, j as __mf_7 } from './_virtual_mf___mfe_internal__CasRestore__mf_owner__258351331282643__loadShare__vue__loadShare__-CJ5-BD8a.js';

const _hoisted_1 = { class: "pa-4" };


const _sfc_main = {
  __name: 'Config',
  props: {
  api: { type: Object, default: () => ({}) },
  pluginId: { type: String, default: '' },
},
  setup(__props) {

const props = __props;

const cfg = __mf_1({
  enabled: false,
  watch_dir: '',
  account_id: '',
  scope: 'personal',
  family_id: '',
  target_folder_path: '',
  notify: true,
  onlyonce: false,
});

const accounts = __mf_1([]);
const families = __mf_1([]);
const accountHint = __mf_1('');
const familyHint = __mf_1('');
const loading = __mf_1(false);
const saving = __mf_1(false);

async function loadConfig() {
  try {
    const r = await props.api.get(`plugin/${props.pluginId}/config`);
    if (r && r.config && typeof r.config === 'object') {
      cfg.value = { ...cfg.value, ...r.config };
    }
  } catch (e) {
    console.error('CasRestore load config failed', e);
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

async function save() {
  saving.value = true;
  try {
    await props.api.post(`plugin/${props.pluginId}/save_config`, cfg.value);
  } catch (e) {
    console.error('CasRestore save config failed', e);
  } finally {
    saving.value = false;
  }
}

__mf_13(() => cfg.value.account_id, loadFamilies);
__mf_13(() => cfg.value.scope, loadFamilies);

__mf_9(async () => {
  loading.value = true;
  try {
    await loadConfig();
    await loadAccounts();
    await loadFamilies();
  } finally {
    loading.value = false;
  }
});

return (_ctx, _cache) => {
  const _component_v_card_title = __mf_12("v-card-title");
  const _component_v_alert = __mf_12("v-alert");
  const _component_v_switch = __mf_12("v-switch");
  const _component_v_col = __mf_12("v-col");
  const _component_v_text_field = __mf_12("v-text-field");
  const _component_v_select = __mf_12("v-select");
  const _component_v_row = __mf_12("v-row");
  const _component_v_card_text = __mf_12("v-card-text");
  const _component_v_spacer = __mf_12("v-spacer");
  const _component_v_btn = __mf_12("v-btn");
  const _component_v_card_actions = __mf_12("v-card-actions");
  const _component_v_card = __mf_12("v-card");

  return (__mf_10(), __mf_5("div", _hoisted_1, [
    __mf_8(_component_v_card, null, {
      default: __mf_14(() => [
        __mf_8(_component_v_card_title, null, {
          default: __mf_14(() => [...(_cache[8] || (_cache[8] = [
            __mf_7("CAS 恢复源文件 · 设置", -1)
          ]))]),
          _: 1
        }),
        __mf_8(_component_v_card_text, null, {
          default: __mf_14(() => [
            __mf_8(_component_v_alert, {
              type: "info",
              variant: "tonal",
              density: "compact",
              class: "mb-4"
            }, {
              default: __mf_14(() => [...(_cache[9] || (_cache[9] = [
                __mf_7(" 监控目录有新 .cas 写入时自动恢复；CAS 服务地址与 API Key 读取「CAS助手」插件，账号 / 家庭 / 目标路径在此选择。 ", -1)
              ]))]),
              _: 1
            }),
            __mf_8(_component_v_row, null, {
              default: __mf_14(() => [
                __mf_8(_component_v_col, { cols: "12" }, {
                  default: __mf_14(() => [
                    __mf_8(_component_v_switch, {
                      modelValue: cfg.value.enabled,
                      "onUpdate:modelValue": _cache[0] || (_cache[0] = $event => ((cfg.value.enabled) = $event)),
                      label: "启用插件",
                      density: "compact",
                      "hide-details": ""
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                }),
                __mf_8(_component_v_col, { cols: "12" }, {
                  default: __mf_14(() => [
                    __mf_8(_component_v_text_field, {
                      modelValue: cfg.value.watch_dir,
                      "onUpdate:modelValue": _cache[1] || (_cache[1] = $event => ((cfg.value.watch_dir) = $event)),
                      label: "监控目录",
                      hint: "容器内绝对路径（含子目录）",
                      "persistent-hint": "",
                      density: "compact"
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                }),
                __mf_8(_component_v_col, {
                  cols: "12",
                  md: "4"
                }, {
                  default: __mf_14(() => [
                    __mf_8(_component_v_select, {
                      modelValue: cfg.value.account_id,
                      "onUpdate:modelValue": _cache[2] || (_cache[2] = $event => ((cfg.value.account_id) = $event)),
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
                __mf_8(_component_v_col, {
                  cols: "12",
                  md: "4"
                }, {
                  default: __mf_14(() => [
                    __mf_8(_component_v_select, {
                      modelValue: cfg.value.scope,
                      "onUpdate:modelValue": _cache[3] || (_cache[3] = $event => ((cfg.value.scope) = $event)),
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
                __mf_8(_component_v_col, {
                  cols: "12",
                  md: "4"
                }, {
                  default: __mf_14(() => [
                    __mf_8(_component_v_select, {
                      modelValue: cfg.value.family_id,
                      "onUpdate:modelValue": _cache[4] || (_cache[4] = $event => ((cfg.value.family_id) = $event)),
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
                __mf_8(_component_v_col, { cols: "12" }, {
                  default: __mf_14(() => [
                    __mf_8(_component_v_text_field, {
                      modelValue: cfg.value.target_folder_path,
                      "onUpdate:modelValue": _cache[5] || (_cache[5] = $event => ((cfg.value.target_folder_path) = $event)),
                      label: "目标文件夹路径",
                      hint: "网盘基路径（如 家庭1/测试），本地子目录自动拼接到其后",
                      "persistent-hint": "",
                      density: "compact"
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                }),
                __mf_8(_component_v_col, {
                  cols: "12",
                  md: "6"
                }, {
                  default: __mf_14(() => [
                    __mf_8(_component_v_switch, {
                      modelValue: cfg.value.notify,
                      "onUpdate:modelValue": _cache[6] || (_cache[6] = $event => ((cfg.value.notify) = $event)),
                      label: "发送通知",
                      density: "compact",
                      "hide-details": ""
                    }, null, 8, ["modelValue"])
                  ]),
                  _: 1
                }),
                __mf_8(_component_v_col, {
                  cols: "12",
                  md: "6"
                }, {
                  default: __mf_14(() => [
                    __mf_8(_component_v_switch, {
                      modelValue: cfg.value.onlyonce,
                      "onUpdate:modelValue": _cache[7] || (_cache[7] = $event => ((cfg.value.onlyonce) = $event)),
                      label: "立即运行一次",
                      density: "compact",
                      "hide-details": ""
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
        __mf_8(_component_v_card_actions, null, {
          default: __mf_14(() => [
            __mf_8(_component_v_spacer),
            __mf_8(_component_v_btn, {
              color: "primary",
              loading: saving.value,
              onClick: save
            }, {
              default: __mf_14(() => [...(_cache[10] || (_cache[10] = [
                __mf_7("保存", -1)
              ]))]),
              _: 1
            }, 8, ["loading"])
          ]),
          _: 1
        })
      ]),
      _: 1
    })
  ]))
}
}

};

export { _sfc_main as default };
