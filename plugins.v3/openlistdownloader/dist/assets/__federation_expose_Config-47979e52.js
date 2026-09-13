import { importShared } from './__federation_fn_import-054b33c3.js';
import { _ as _export_sfc } from './_plugin-vue_export-helper-c4c0bc37.js';

const {defineComponent:_defineComponent} = await importShared('vue');

const {resolveComponent:_resolveComponent,createVNode:_createVNode,createTextVNode:_createTextVNode,createElementVNode:_createElementVNode,renderList:_renderList,Fragment:_Fragment,openBlock:_openBlock,createElementBlock:_createElementBlock,withCtx:_withCtx,createBlock:_createBlock} = await importShared('vue');

const _hoisted_1 = { class: "pa-4 config-page" };
const _hoisted_2 = { class: "text-h6 font-weight-bold mb-4" };
const _hoisted_3 = { class: "d-flex justify-end mt-4" };
const {ref,onMounted} = await importShared('vue');

const PLUGIN_ID = "OpenListDownloader";
const _sfc_main = /* @__PURE__ */ _defineComponent({
  __name: "Config",
  props: {
    // kebab-case "initial-config" 会被 Vue 映射为 initialConfig
    initialConfig: { type: Object, default: () => ({}) },
    api: { type: Object, default: () => ({}) },
    pluginId: { type: String, default: "" }
  },
  emits: ["save", "layout", "switch", "close"],
  setup(__props, { emit: __emit }) {
    const props = __props;
    const emit = __emit;
    const form = ref({ ...props.initialConfig || {} });
    const wxChannels = ref([]);
    const wxLoading = ref(false);
    const CHANNEL_TYPE_LABEL = {
      wechat: "企业微信",
      wechatclawbot: "微信机器人",
      telegram: "Telegram",
      feishu: "飞书",
      slack: "Slack",
      discord: "Discord",
      vocechat: "VoceChat",
      synologychat: "SynologyChat",
      qqbot: "QQ"
    };
    function channelLabel(c) {
      const t = CHANNEL_TYPE_LABEL[c.type] || c.type || "未知";
      return `${c.name}（${t}）`;
    }
    async function loadChannels() {
      wxLoading.value = true;
      try {
        let data = null;
        if (props.api && typeof props.api.get === "function") {
          data = await props.api.get(`plugin/${PLUGIN_ID}/channels`);
        } else {
          const res = await fetch(`/api/v1/plugin/${PLUGIN_ID}/channels`);
          data = await res.json();
        }
        const list = data?.channels || (Array.isArray(data) ? data : []);
        wxChannels.value = (list || []).filter((c) => c && c.name).map((c) => ({
          name: c.name,
          type: c.type || ""
        }));
      } catch (e) {
        wxChannels.value = [];
      } finally {
        wxLoading.value = false;
      }
    }
    const switches = [
      { key: "enabled", label: "启用插件" },
      { key: "onlyonce", label: "立即运行一次" },
      { key: "clear_records", label: "清空记录" },
      { key: "notify", label: "发送通知" }
    ];
    const inputs = [
      { key: "openlist_url", label: "OpenList 地址", type: "text", placeholder: "http://192.168.2.6:5244" },
      { key: "openlist_token", label: "OpenList Token", type: "text", placeholder: "API 令牌 / JWT" },
      { key: "downloader", label: "下载器名称", type: "text", placeholder: "Aria2" },
      { key: "wx_channel", label: "通知渠道（留空=MP默认通知）", type: "channel", placeholder: "选择系统已配置的通知渠道" },
      { key: "download_dir", label: "Aria2 下载保存目录", type: "text", placeholder: "/downloads" },
      { key: "file_ext", label: "仅下载后缀（逗号分隔，空=全部）", type: "text", placeholder: "mkv,mp4" },
      { key: "min_size", label: "最小文件大小（MB，0=不限）", type: "number", placeholder: "0" },
      { key: "max_depth", label: "递归深度（0=仅当前层）", type: "number", placeholder: "0" },
      { key: "max_files_per_round", label: "单轮最多下载数", type: "number", placeholder: "50" },
      {
        key: "cron_expression",
        label: "定时表达式",
        type: "select",
        options: [
          { value: "*/10 * * * *", text: "每 10 分钟" },
          { value: "*/30 * * * *", text: "每 30 分钟" },
          { value: "0 * * * *", text: "每小时" },
          { value: "0 */2 * * *", text: "每 2 小时" },
          { value: "0 */4 * * *", text: "每 4 小时" },
          { value: "0 0 * * *", text: "每天" },
          { value: "0 2 * * *", text: "每天凌晨 2 点" },
          { value: "0 0 * * 0", text: "每周日" },
          { value: "20 7-11 * * *", text: "每天下午7:20-11:20" }
        ]
      },
      { key: "monitor_paths", label: "监控路径（每行一个）", type: "textarea", placeholder: "/电影\n/剧集/更新", rows: 4 }
    ];
    function onCronChange(f, v) {
      if (v && typeof v === "object" && v.value !== void 0) {
        form.value[f.key] = v.value;
      } else {
        form.value[f.key] = v ?? "";
      }
    }
    onMounted(() => {
      form.value = { ...props.initialConfig || {} };
      loadChannels();
    });
    function save() {
      emit("save", { ...form.value });
    }
    return (_ctx, _cache) => {
      const _component_v_btn = _resolveComponent("v-btn");
      const _component_v_icon = _resolveComponent("v-icon");
      const _component_v_switch = _resolveComponent("v-switch");
      const _component_v_card = _resolveComponent("v-card");
      const _component_v_col = _resolveComponent("v-col");
      const _component_v_row = _resolveComponent("v-row");
      const _component_v_divider = _resolveComponent("v-divider");
      const _component_v_textarea = _resolveComponent("v-textarea");
      const _component_v_text_field = _resolveComponent("v-text-field");
      const _component_v_combobox = _resolveComponent("v-combobox");
      const _component_v_select = _resolveComponent("v-select");
      return _openBlock(), _createElementBlock("div", _hoisted_1, [
        _createVNode(_component_v_btn, {
          icon: "mdi-close",
          variant: "text",
          size: "small",
          class: "config-close",
          onClick: _cache[0] || (_cache[0] = ($event) => emit("close"))
        }),
        _createElementVNode("div", _hoisted_2, [
          _createVNode(_component_v_icon, {
            icon: "mdi-cog-outline",
            class: "mr-2"
          }),
          _cache[1] || (_cache[1] = _createTextVNode(" OpenList 自动下载 · 设置 ", -1))
        ]),
        _createVNode(_component_v_row, null, {
          default: _withCtx(() => [
            (_openBlock(), _createElementBlock(_Fragment, null, _renderList(switches, (f) => {
              return _createVNode(_component_v_col, {
                key: f.key,
                cols: "12",
                sm: "6",
                md: "3"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_card, {
                    variant: "tonal",
                    flat: "",
                    class: "px-2 py-1 mb-2"
                  }, {
                    default: _withCtx(() => [
                      _createVNode(_component_v_switch, {
                        "model-value": !!form.value[f.key],
                        label: f.label,
                        color: "primary",
                        density: "comfortable",
                        "hide-details": "",
                        "onUpdate:modelValue": (v) => form.value[f.key] = !!v
                      }, null, 8, ["model-value", "label", "onUpdate:modelValue"])
                    ]),
                    _: 2
                  }, 1024)
                ]),
                _: 2
              }, 1024);
            }), 64))
          ]),
          _: 1
        }),
        _createVNode(_component_v_divider, { class: "my-4" }),
        _createVNode(_component_v_row, null, {
          default: _withCtx(() => [
            (_openBlock(), _createElementBlock(_Fragment, null, _renderList(inputs, (f) => {
              return _createVNode(_component_v_col, {
                key: f.key,
                cols: "12",
                md: "6"
              }, {
                default: _withCtx(() => [
                  f.type === "textarea" ? (_openBlock(), _createBlock(_component_v_textarea, {
                    key: 0,
                    "model-value": form.value[f.key] ?? "",
                    label: f.label,
                    placeholder: f.placeholder,
                    rows: f.rows || 3,
                    "auto-grow": "",
                    "hide-details": false,
                    variant: "outlined",
                    density: "comfortable",
                    "onUpdate:modelValue": (v) => form.value[f.key] = v
                  }, null, 8, ["model-value", "label", "placeholder", "rows", "onUpdate:modelValue"])) : f.type === "number" ? (_openBlock(), _createBlock(_component_v_text_field, {
                    key: 1,
                    "model-value": form.value[f.key] ?? 0,
                    label: f.label,
                    placeholder: f.placeholder,
                    type: "number",
                    variant: "outlined",
                    density: "comfortable",
                    "onUpdate:modelValue": (v) => form.value[f.key] = Number(v) || 0
                  }, null, 8, ["model-value", "label", "placeholder", "onUpdate:modelValue"])) : f.type === "select" ? (_openBlock(), _createBlock(_component_v_combobox, {
                    key: 2,
                    "model-value": form.value[f.key] ?? "",
                    label: f.label,
                    items: f.options,
                    "item-title": "text",
                    "item-value": "value",
                    variant: "outlined",
                    density: "comfortable",
                    "hide-details": false,
                    "onUpdate:modelValue": (v) => onCronChange(f, v)
                  }, null, 8, ["model-value", "label", "items", "onUpdate:modelValue"])) : f.type === "channel" ? (_openBlock(), _createBlock(_component_v_select, {
                    key: 3,
                    "model-value": form.value[f.key] ?? "",
                    label: f.label,
                    items: wxChannels.value,
                    "item-title": (c) => channelLabel(c),
                    "item-value": "name",
                    loading: wxLoading.value,
                    placeholder: f.placeholder,
                    clearable: "",
                    variant: "outlined",
                    density: "comfortable",
                    "hide-details": false,
                    "onUpdate:modelValue": (v) => form.value[f.key] = v || ""
                  }, null, 8, ["model-value", "label", "items", "item-title", "loading", "placeholder", "onUpdate:modelValue"])) : (_openBlock(), _createBlock(_component_v_text_field, {
                    key: 4,
                    "model-value": form.value[f.key] ?? "",
                    label: f.label,
                    placeholder: f.placeholder,
                    variant: "outlined",
                    density: "comfortable",
                    "onUpdate:modelValue": (v) => form.value[f.key] = v
                  }, null, 8, ["model-value", "label", "placeholder", "onUpdate:modelValue"]))
                ]),
                _: 2
              }, 1024);
            }), 64))
          ]),
          _: 1
        }),
        _createElementVNode("div", _hoisted_3, [
          _createVNode(_component_v_btn, {
            color: "primary",
            "prepend-icon": "mdi-content-save",
            onClick: save
          }, {
            default: _withCtx(() => [..._cache[2] || (_cache[2] = [
              _createTextVNode(" 保存 ", -1)
            ])]),
            _: 1
          })
        ])
      ]);
    };
  }
});

const Config_vue_vue_type_style_index_0_scoped_aeb37177_lang = '';

const Config = /* @__PURE__ */ _export_sfc(_sfc_main, [["__scopeId", "data-v-aeb37177"]]);

export { Config as default };
