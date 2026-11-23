import { c as __mf_1, d as __mf_9, e as __mf_12, _ as __mf_10, a as __mf_5, f as __mf_8, g as __mf_14, h as __mf_6, i as __mf_2, j as __mf_7, k as __mf_3, l as __mf_4, m as __mf_11 } from './_virtual_mf___mfe_internal__CasRestore__mf_owner__258351331282643__loadShare__vue__loadShare__-CJ5-BD8a.js';

const _hoisted_1 = { class: "pa-4" };
const _hoisted_2 = {
  class: "d-flex align-center",
  style: {"gap":"8px"}
};
const _hoisted_3 = { class: "text-body-2 mb-2 text-medium-emphasis" };
const _hoisted_4 = { key: 0 };
const _hoisted_5 = { class: "text-no-wrap" };


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

__mf_9(load);

return (_ctx, _cache) => {
  const _component_v_btn = __mf_12("v-btn");
  const _component_v_card_title = __mf_12("v-card-title");
  const _component_v_chip = __mf_12("v-chip");
  const _component_v_table = __mf_12("v-table");
  const _component_v_card_text = __mf_12("v-card-text");
  const _component_v_card = __mf_12("v-card");

  return (__mf_10(), __mf_5("div", _hoisted_1, [
    __mf_8(_component_v_card, null, {
      default: __mf_14(() => [
        __mf_8(_component_v_card_title, { class: "d-flex align-center justify-space-between" }, {
          default: __mf_14(() => [
            __mf_6("div", null, " CAS 恢复（共 " + __mf_2(data.value.total) + " 条 · 成功 " + __mf_2(data.value.successCount) + " · 失败 " + __mf_2(data.value.failedCount) + "） ", 1),
            __mf_6("div", _hoisted_2, [
              __mf_8(_component_v_btn, {
                size: "small",
                variant: "outlined",
                color: "primary",
                loading: loading.value,
                onClick: _cache[0] || (_cache[0] = $event => (action('scan')))
              }, {
                default: __mf_14(() => [...(_cache[3] || (_cache[3] = [
                  __mf_7(" 立即扫描 ", -1)
                ]))]),
                _: 1
              }, 8, ["loading"]),
              __mf_8(_component_v_btn, {
                size: "small",
                variant: "outlined",
                color: "info",
                onClick: _cache[1] || (_cache[1] = $event => (action('test')))
              }, {
                default: __mf_14(() => [...(_cache[4] || (_cache[4] = [
                  __mf_7(" 测试连接 ", -1)
                ]))]),
                _: 1
              }),
              __mf_8(_component_v_btn, {
                size: "small",
                variant: "outlined",
                color: "error",
                onClick: _cache[2] || (_cache[2] = $event => (action('clear_records')))
              }, {
                default: __mf_14(() => [...(_cache[5] || (_cache[5] = [
                  __mf_7(" 清空记录 ", -1)
                ]))]),
                _: 1
              })
            ])
          ]),
          _: 1
        }),
        __mf_8(_component_v_card_text, null, {
          default: __mf_14(() => [
            __mf_6("div", _hoisted_3, [
              __mf_7(" 状态：" + __mf_2(data.value.scanning ? '扫描中' : '空闲') + " · 启用=" + __mf_2(data.value.enabled) + " · 上次扫描=" + __mf_2(data.value.last_scan || '从未') + " ", 1),
              (data.value.last_result)
                ? (__mf_10(), __mf_5(__mf_3, { key: 0 }, [
                    __mf_7(" · " + __mf_2(data.value.last_result), 1)
                  ], 64))
                : __mf_4("", true)
            ]),
            __mf_8(_component_v_table, {
              density: "comfortable",
              hover: "",
              "fixed-header": "",
              height: "600px"
            }, {
              default: __mf_14(() => [
                _cache[7] || (_cache[7] = __mf_6("thead", null, [
                  __mf_6("tr", null, [
                    __mf_6("th", {
                      class: "text-no-wrap",
                      style: {"width":"170px"}
                    }, "时间"),
                    __mf_6("th", null, "本地文件"),
                    __mf_6("th", null, "恢复到网盘"),
                    __mf_6("th", { style: {"width":"90px"} }, "状态"),
                    __mf_6("th", null, "说明")
                  ])
                ], -1)),
                __mf_6("tbody", null, [
                  (!data.value.records.length)
                    ? (__mf_10(), __mf_5("tr", _hoisted_4, [...(_cache[6] || (_cache[6] = [
                        __mf_6("td", {
                          colspan: "5",
                          class: "text-center text-medium-emphasis"
                        }, "暂无恢复记录", -1)
                      ]))]))
                    : __mf_4("", true),
                  (__mf_10(true), __mf_5(__mf_3, null, __mf_11(data.value.records, (r, i) => {
                    return (__mf_10(), __mf_5("tr", { key: i }, [
                      __mf_6("td", _hoisted_5, __mf_2(r.time), 1),
                      __mf_6("td", null, __mf_2(r.filename), 1),
                      __mf_6("td", null, __mf_2(r.target), 1),
                      __mf_6("td", null, [
                        __mf_8(_component_v_chip, {
                          size: "small",
                          color: r.status === '成功' ? 'success' : 'error'
                        }, {
                          default: __mf_14(() => [
                            __mf_7(__mf_2(r.status), 1)
                          ]),
                          _: 2
                        }, 1032, ["color"])
                      ]),
                      __mf_6("td", null, __mf_2(r.message), 1)
                    ]))
                  }), 128))
                ])
              ]),
              _: 1
            })
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
