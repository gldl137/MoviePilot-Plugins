import { importShared } from './__federation_fn_import-054b33c3.js';
import { _ as _export_sfc } from './_plugin-vue_export-helper-c4c0bc37.js';

const {defineComponent:_defineComponent$1} = await importShared('vue');

const {resolveComponent:_resolveComponent,createVNode:_createVNode,openBlock:_openBlock$1,createBlock:_createBlock$1,createCommentVNode:_createCommentVNode,toDisplayString:_toDisplayString,createTextVNode:_createTextVNode,withCtx:_withCtx,createElementVNode:_createElementVNode,createElementBlock:_createElementBlock} = await importShared('vue');

const _hoisted_1 = { class: "dash-page" };
const _hoisted_2 = { class: "dash-content" };
const _hoisted_3 = { class: "dash-sub-num" };
const _hoisted_4 = { class: "dash-sub-num" };
const _hoisted_5 = {
  class: "dash-sub-num",
  style: { "font-size": "16px" }
};
const _hoisted_6 = { class: "dash-actions" };
const {ref,onMounted} = await importShared('vue');

const PLUGIN_ID = "OpenListDownloader";
const _sfc_main$1 = /* @__PURE__ */ _defineComponent$1({
  __name: "Dashboard",
  props: {
    navKey: { type: String, default: "main" },
    initialConfig: { type: Object, default: () => ({}) },
    api: { type: Object, default: () => ({}) },
    pluginId: { type: String, default: "" }
  },
  emits: ["switch", "action", "close"],
  setup(__props, { emit: __emit }) {
    const props = __props;
    const emit = __emit;
    const loading = ref(false);
    const clearing = ref(false);
    const error = ref("");
    const notice = ref("");
    const noticeVisible = ref(false);
    let noticeTimer = null;
    const stats = ref({
      total: 0,
      last_time: "",
      total_size: 0
    });
    function humanSize(bytes) {
      if (!bytes || bytes <= 0)
        return "0 B";
      const units = ["B", "KB", "MB", "GB", "TB"];
      let size = Number(bytes);
      let i = 0;
      while (size >= 1024 && i < units.length - 1) {
        size /= 1024;
        i++;
      }
      return `${size.toFixed(i === 0 ? 0 : 2)} ${units[i]}`;
    }
    async function loadStats() {
      loading.value = true;
      error.value = "";
      try {
        let data = null;
        if (props.api && typeof props.api.get === "function") {
          const res = await props.api.get(`plugin/${PLUGIN_ID}/history`);
          data = res;
        } else {
          const res = await fetch(`/api/v1/plugin/${PLUGIN_ID}/history`);
          data = await res.json();
        }
        const list = data?.items || data?.data || (Array.isArray(data) ? data : []);
        const total = Number(data?.count ?? list?.length ?? 0);
        const last = list && list.length ? list[0] : null;
        const totalSize = (list || []).reduce((acc, it) => acc + (Number(it.size) || 0), 0);
        stats.value = {
          total,
          last_time: last?.time || "",
          total_size: totalSize
        };
      } catch (e) {
        error.value = String(e?.message || e);
      } finally {
        loading.value = false;
      }
    }
    function showNotice(msg) {
      notice.value = msg;
      noticeVisible.value = true;
      if (noticeTimer)
        clearTimeout(noticeTimer);
      noticeTimer = setTimeout(() => {
        noticeVisible.value = false;
      }, 2500);
    }
    async function clearRecords() {
      clearing.value = true;
      try {
        let res = null;
        if (props.api && typeof props.api.post === "function") {
          res = await props.api.post(`plugin/${PLUGIN_ID}/clear_records`, {});
        } else {
          const r = await fetch(`/api/v1/plugin/${PLUGIN_ID}/clear_records`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({})
          });
          res = await r.json();
        }
        if (res?.success === false) {
          showNotice(res?.message || "清除失败");
        } else {
          showNotice(res?.message || "已清除");
          await loadStats();
        }
      } catch (e) {
        showNotice(String(e?.message || e));
      } finally {
        clearing.value = false;
      }
    }
    onMounted(loadStats);
    return (_ctx, _cache) => {
      const _component_v_btn = _resolveComponent("v-btn");
      const _component_v_progress_linear = _resolveComponent("v-progress-linear");
      const _component_v_alert = _resolveComponent("v-alert");
      const _component_v_card = _resolveComponent("v-card");
      const _component_v_col = _resolveComponent("v-col");
      const _component_v_row = _resolveComponent("v-row");
      const _component_v_snackbar = _resolveComponent("v-snackbar");
      return _openBlock$1(), _createElementBlock("div", _hoisted_1, [
        _createVNode(_component_v_btn, {
          icon: "mdi-close",
          variant: "text",
          size: "small",
          class: "dash-close",
          onClick: _cache[0] || (_cache[0] = ($event) => emit("close"))
        }),
        loading.value ? (_openBlock$1(), _createBlock$1(_component_v_progress_linear, {
          key: 0,
          indeterminate: "",
          color: "primary"
        })) : _createCommentVNode("", true),
        error.value ? (_openBlock$1(), _createBlock$1(_component_v_alert, {
          key: 1,
          type: "error",
          variant: "tonal",
          class: "my-3"
        }, {
          default: _withCtx(() => [
            _createTextVNode(_toDisplayString(error.value), 1)
          ]),
          _: 1
        })) : _createCommentVNode("", true),
        _createElementVNode("div", _hoisted_2, [
          _createVNode(_component_v_row, { class: "dash-sub" }, {
            default: _withCtx(() => [
              _createVNode(_component_v_col, {
                cols: "12",
                md: "4"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_card, {
                    variant: "tonal",
                    flat: "",
                    class: "dash-sub-card"
                  }, {
                    default: _withCtx(() => [
                      _cache[3] || (_cache[3] = _createElementVNode("div", { class: "dash-sub-label" }, "总下载文件数", -1)),
                      _createElementVNode("div", _hoisted_3, _toDisplayString(stats.value.total), 1)
                    ]),
                    _: 1
                  })
                ]),
                _: 1
              }),
              _createVNode(_component_v_col, {
                cols: "12",
                md: "4"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_card, {
                    variant: "tonal",
                    flat: "",
                    class: "dash-sub-card"
                  }, {
                    default: _withCtx(() => [
                      _cache[4] || (_cache[4] = _createElementVNode("div", { class: "dash-sub-label" }, "累计下载大小", -1)),
                      _createElementVNode("div", _hoisted_4, _toDisplayString(humanSize(stats.value.total_size)), 1)
                    ]),
                    _: 1
                  })
                ]),
                _: 1
              }),
              _createVNode(_component_v_col, {
                cols: "12",
                md: "4"
              }, {
                default: _withCtx(() => [
                  _createVNode(_component_v_card, {
                    variant: "tonal",
                    flat: "",
                    class: "dash-sub-card"
                  }, {
                    default: _withCtx(() => [
                      _cache[5] || (_cache[5] = _createElementVNode("div", { class: "dash-sub-label" }, "最近下载时间", -1)),
                      _createElementVNode("div", _hoisted_5, _toDisplayString(stats.value.last_time || "-"), 1)
                    ]),
                    _: 1
                  })
                ]),
                _: 1
              })
            ]),
            _: 1
          })
        ]),
        _createElementVNode("div", _hoisted_6, [
          _createVNode(_component_v_btn, {
            color: "primary",
            variant: "tonal",
            "prepend-icon": "mdi-cog-outline",
            class: "mr-2",
            onClick: _cache[1] || (_cache[1] = ($event) => emit("switch"))
          }, {
            default: _withCtx(() => [..._cache[6] || (_cache[6] = [
              _createTextVNode(" 设置 ", -1)
            ])]),
            _: 1
          }),
          _createVNode(_component_v_btn, {
            color: "error",
            variant: "tonal",
            "prepend-icon": "mdi-delete-sweep-outline",
            loading: clearing.value,
            onClick: clearRecords
          }, {
            default: _withCtx(() => [..._cache[7] || (_cache[7] = [
              _createTextVNode(" 清除 ", -1)
            ])]),
            _: 1
          }, 8, ["loading"])
        ]),
        _createVNode(_component_v_snackbar, {
          modelValue: noticeVisible.value,
          "onUpdate:modelValue": _cache[2] || (_cache[2] = ($event) => noticeVisible.value = $event),
          color: "success",
          location: "bottom",
          timeout: "2500"
        }, {
          default: _withCtx(() => [
            _createTextVNode(_toDisplayString(notice.value), 1)
          ]),
          _: 1
        }, 8, ["modelValue"])
      ]);
    };
  }
});

const Dashboard_vue_vue_type_style_index_0_scoped_78a80bce_lang = '';

const Dashboard = /* @__PURE__ */ _export_sfc(_sfc_main$1, [["__scopeId", "data-v-78a80bce"]]);

const {defineComponent:_defineComponent} = await importShared('vue');

const {openBlock:_openBlock,createBlock:_createBlock} = await importShared('vue');
const _sfc_main = /* @__PURE__ */ _defineComponent({
  __name: "Page",
  setup(__props) {
    return (_ctx, _cache) => {
      return _openBlock(), _createBlock(Dashboard);
    };
  }
});

export { _sfc_main as default };
