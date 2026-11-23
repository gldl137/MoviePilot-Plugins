import { c as __mf_1, d as __mf_5, e as __mf_12, f as __mf_15, _ as __mf_13, a as __mf_8, g as __mf_9, h as __mf_10, i as __mf_3, j as __mf_4, k as __mf_7, l as __mf_11, m as __mf_17, n as __mf_14, o as __mf_6 } from './_virtual_mf___mfe_internal__CasRestore__mf_owner__258351331282643__loadShare__vue__loadShare__-De8H7wIV.js';
import { _ as _export_sfc } from './_plugin-vue_export-helper-pcqpp-6-.js';

const _hoisted_1 = { class: "pa-4" };
const _hoisted_2 = {
  class: "d-flex align-start justify-space-between flex-wrap mb-3",
  style: {"gap":"8px"}
};
const _hoisted_3 = { class: "min-w-0" };
const _hoisted_4 = { class: "text-body-2 text-medium-emphasis mt-1" };
const _hoisted_5 = { class: "text-caption text-medium-emphasis mt-1" };
const _hoisted_6 = {
  class: "d-flex align-center flex-wrap",
  style: {"gap":"8px"}
};
const _hoisted_7 = {
  key: 0,
  class: "text-center text-medium-emphasis py-12"
};
const _hoisted_8 = ["title"];
const _hoisted_9 = { class: "flex-grow-1 min-w-0" };
const _hoisted_10 = ["title"];
const _hoisted_11 = ["title"];
const _hoisted_12 = {
  key: 2,
  class: "record-path text-truncate"
};
const _hoisted_13 = { class: "record-status flex-shrink-0 ml-3" };
const _hoisted_14 = {
  class: "d-flex flex-column align-end flex-shrink-0 ml-3",
  style: {"gap":"3px"}
};
const _hoisted_15 = {
  class: "d-flex align-center",
  style: {"gap":"6px"}
};
const _hoisted_16 = {
  key: 0,
  class: "record-meta"
};
const _hoisted_17 = { class: "record-meta" };
const _hoisted_18 = {
  class: "d-flex align-center justify-space-between flex-wrap mt-4",
  style: {"gap":"8px"}
};
const _hoisted_19 = { class: "text-body-2 text-medium-emphasis" };


const _sfc_main = {
  __name: 'AppPage',
  props: {
  api: { type: Object, default: () => ({}) },
  pluginId: { type: String, default: '' },
  navKey: { type: String, default: 'main' },
  sourcePluginId: { type: String, default: '' },
  nativeSubscribe: { type: Function, default: null },
},
  setup(__props) {

const props = __props;

const loading = __mf_1(false);
const data = __mf_1({
  records: [],
  total: 0,
  successCount: 0,
  failedCount: 0,
  enabled: false,
  scanning: false,
  last_scan: '',
  last_result: '',
});

// 分页（前端分页，后端 records 最多 200 条）
const page = __mf_1(1);
const itemsPerPage = __mf_1(50);
const perPageOptions = [
  { title: '10 条/页', value: 10 },
  { title: '20 条/页', value: 20 },
  { title: '50 条/页', value: 50 },
  { title: '100 条/页', value: 100 },
];
const totalPages = __mf_5(() =>
  Math.max(1, Math.ceil(data.value.records.length / itemsPerPage.value)));
const pagedRecords = __mf_5(() => {
  const start = (page.value - 1) * itemsPerPage.value;
  return data.value.records.slice(start, start + itemsPerPage.value)
});
const rangeText = __mf_5(() => {
  if (!data.value.records.length) return '0 条'
  const start = (page.value - 1) * itemsPerPage.value + 1;
  const end = Math.min(page.value * itemsPerPage.value, data.value.records.length);
  return `${start}-${end} / ${data.value.records.length}`
});

function relativeTime(text) {
  if (!text) return ''
  const t = new Date(String(text).replace(/-/g, '/')).getTime();
  if (Number.isNaN(t)) return text
  const diff = Math.max(0, Date.now() - t);
  const min = Math.floor(diff / 60000);
  if (min < 1) return '刚刚'
  if (min < 60) return `${min}分钟前`
  const hour = Math.floor(min / 60);
  if (hour < 24) return `${hour}小时前`
  const day = Math.floor(hour / 24);
  if (day < 30) return `${day}天前`
  return text
}

function rowTitle(r) {
  return r.title || (String(r.filename || '').replace(/\.cas$/i, ''))
}
function rowNetPath(r) {
  return r.net_path || r.target || ''
}

async function load() {
  loading.value = true;
  try {
    const r = await props.api.get(`plugin/${props.pluginId}/records`);
    if (r && typeof r === 'object') {
      data.value = {
        records: r.records || [],
        total: r.total || 0,
        successCount: r.successCount || 0,
        failedCount: r.failedCount || 0,
        enabled: !!r.enabled,
        scanning: !!r.scanning,
        last_scan: r.last_scan || '',
        last_result: r.last_result || '',
      };
      if (page.value > totalPages.value) page.value = 1;
    }
  } catch (e) {
    console.error('CasRestore load records failed', e);
  } finally {
    loading.value = false;
  }
}

async function action(endpoint) {
  try {
    await props.api.post(`plugin/${props.pluginId}/${endpoint}`);
  } catch (e) {
    console.error('CasRestore action failed', endpoint, e);
  }
  await load();
}

__mf_12(load);

return (_ctx, _cache) => {
  const _component_v_btn = __mf_15("v-btn");
  const _component_v_divider = __mf_15("v-divider");
  const _component_v_icon = __mf_15("v-icon");
  const _component_v_chip = __mf_15("v-chip");
  const _component_v_select = __mf_15("v-select");
  const _component_v_pagination = __mf_15("v-pagination");

  return (__mf_13(), __mf_8("div", _hoisted_1, [
    __mf_9("div", _hoisted_2, [
      __mf_9("div", _hoisted_3, [
        _cache[6] || (_cache[6] = __mf_9("div", { class: "text-subtitle-2 text-primary" }, "媒体整理", -1)),
        _cache[7] || (_cache[7] = __mf_9("div", { class: "text-h6 mt-1" }, "CAS 恢复", -1)),
        __mf_9("div", _hoisted_4, [
          __mf_10(" 共 " + __mf_3(data.value.total) + " 条记录 · 成功 " + __mf_3(data.value.successCount) + " · 失败 " + __mf_3(data.value.failedCount) + " ", 1),
          (data.value.scanning)
            ? (__mf_13(), __mf_8(__mf_4, { key: 0 }, [
                __mf_10(" · 扫描中…")
              ], 64))
            : __mf_7("", true)
        ]),
        __mf_9("div", _hoisted_5, [
          __mf_10(" 状态：" + __mf_3(data.value.scanning ? '扫描中' : '空闲') + " · 启用=" + __mf_3(data.value.enabled) + " · 上次扫描=" + __mf_3(data.value.last_scan || '从未') + " ", 1),
          (data.value.last_result)
            ? (__mf_13(), __mf_8(__mf_4, { key: 0 }, [
                __mf_10(" · " + __mf_3(data.value.last_result), 1)
              ], 64))
            : __mf_7("", true)
        ])
      ]),
      __mf_9("div", _hoisted_6, [
        __mf_11(_component_v_btn, {
          size: "small",
          variant: "outlined",
          color: "primary",
          loading: loading.value,
          onClick: _cache[0] || (_cache[0] = $event => (action('scan')))
        }, {
          default: __mf_17(() => [...(_cache[8] || (_cache[8] = [
            __mf_10(" 立即扫描 ", -1)
          ]))]),
          _: 1
        }, 8, ["loading"]),
        __mf_11(_component_v_btn, {
          size: "small",
          variant: "outlined",
          color: "info",
          onClick: _cache[1] || (_cache[1] = $event => (action('test')))
        }, {
          default: __mf_17(() => [...(_cache[9] || (_cache[9] = [
            __mf_10(" 测试连接 ", -1)
          ]))]),
          _: 1
        }),
        __mf_11(_component_v_btn, {
          size: "small",
          variant: "outlined",
          color: "error",
          onClick: _cache[2] || (_cache[2] = $event => (action('clear_records')))
        }, {
          default: __mf_17(() => [...(_cache[10] || (_cache[10] = [
            __mf_10(" 清空记录 ", -1)
          ]))]),
          _: 1
        })
      ])
    ]),
    __mf_11(_component_v_divider, { class: "mb-1" }),
    (!data.value.records.length)
      ? (__mf_13(), __mf_8("div", _hoisted_7, " 暂无恢复记录 "))
      : (__mf_13(), __mf_8(__mf_4, { key: 1 }, [
          (__mf_13(true), __mf_8(__mf_4, null, __mf_14(pagedRecords.value, (r, i) => {
            return (__mf_13(), __mf_8("div", {
              key: `${page.value}-${i}`,
              class: "record-row d-flex align-center"
            }, [
              __mf_11(_component_v_icon, {
                size: "22",
                class: "mr-3 flex-shrink-0",
                color: r.status === '成功' ? 'success' : 'error'
              }, {
                default: __mf_17(() => [
                  __mf_10(__mf_3(r.status === '成功' ? 'mdi-check-circle-outline' : 'mdi-alert-circle-outline'), 1)
                ]),
                _: 2
              }, 1032, ["color"]),
              __mf_9("div", {
                class: "record-name text-truncate",
                title: rowTitle(r)
              }, __mf_3(rowTitle(r)), 9, _hoisted_8),
              __mf_9("div", _hoisted_9, [
                (r.local_path)
                  ? (__mf_13(), __mf_8("div", {
                      key: 0,
                      class: "record-path text-truncate",
                      title: r.local_path
                    }, [
                      _cache[11] || (_cache[11] = __mf_9("span", { class: "path-tag" }, "监控", -1)),
                      __mf_10(__mf_3(r.local_path), 1)
                    ], 8, _hoisted_10))
                  : __mf_7("", true),
                (rowNetPath(r))
                  ? (__mf_13(), __mf_8("div", {
                      key: 1,
                      class: "record-path text-truncate",
                      title: rowNetPath(r)
                    }, [
                      _cache[12] || (_cache[12] = __mf_9("span", { class: "path-tag path-tag-net" }, "网盘", -1)),
                      __mf_10(__mf_3(rowNetPath(r)), 1)
                    ], 8, _hoisted_11))
                  : __mf_7("", true),
                (!r.local_path && !rowNetPath(r) && r.message)
                  ? (__mf_13(), __mf_8("div", _hoisted_12, __mf_3(r.message), 1))
                  : __mf_7("", true)
              ]),
              __mf_9("div", _hoisted_13, [
                __mf_11(_component_v_chip, {
                  size: "x-small",
                  variant: "tonal",
                  color: r.status === '成功' ? 'success' : 'error'
                }, {
                  default: __mf_17(() => [
                    __mf_10(__mf_3(r.status), 1)
                  ]),
                  _: 2
                }, 1032, ["color"])
              ]),
              __mf_9("div", _hoisted_14, [
                (r.mode)
                  ? (__mf_13(), __mf_6(_component_v_chip, {
                      key: 0,
                      size: "x-small",
                      variant: "tonal",
                      color: "primary"
                    }, {
                      default: __mf_17(() => [
                        __mf_10(__mf_3(r.mode), 1)
                      ]),
                      _: 2
                    }, 1024))
                  : __mf_7("", true),
                __mf_9("div", _hoisted_15, [
                  (r.size)
                    ? (__mf_13(), __mf_8("span", _hoisted_16, __mf_3(r.size), 1))
                    : __mf_7("", true),
                  __mf_9("span", _hoisted_17, __mf_3(relativeTime(r.time)), 1)
                ])
              ])
            ]))
          }), 128)),
          __mf_9("div", _hoisted_18, [
            __mf_11(_component_v_select, {
              modelValue: itemsPerPage.value,
              "onUpdate:modelValue": [
                _cache[3] || (_cache[3] = $event => ((itemsPerPage).value = $event)),
                _cache[4] || (_cache[4] = $event => (page.value = 1))
              ],
              items: perPageOptions,
              density: "compact",
              variant: "outlined",
              "hide-details": "",
              style: {"max-width":"130px"}
            }, null, 8, ["modelValue"]),
            __mf_9("span", _hoisted_19, __mf_3(rangeText.value), 1),
            __mf_11(_component_v_pagination, {
              modelValue: page.value,
              "onUpdate:modelValue": _cache[5] || (_cache[5] = $event => ((page).value = $event)),
              length: totalPages.value,
              "total-visible": 7,
              density: "comfortable"
            }, null, 8, ["modelValue", "length"])
          ])
        ], 64))
  ]))
}
}

};
const AppPage = /*#__PURE__*/_export_sfc(_sfc_main, [['__scopeId',"data-v-39c276b7"]]);

export { AppPage as default };
