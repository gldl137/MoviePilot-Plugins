import { importShared } from './__federation_fn_import-054b33c3.js';
import { _ as _export_sfc } from './_plugin-vue_export-helper-c4c0bc37.js';

const {defineComponent:_defineComponent} = await importShared('vue');

const {createTextVNode:_createTextVNode,resolveComponent:_resolveComponent,withCtx:_withCtx,createVNode:_createVNode,createElementVNode:_createElementVNode,toDisplayString:_toDisplayString,openBlock:_openBlock,createBlock:_createBlock,createCommentVNode:_createCommentVNode,createElementBlock:_createElementBlock,renderList:_renderList,Fragment:_Fragment,normalizeStyle:_normalizeStyle,mergeProps:_mergeProps} = await importShared('vue');

const _hoisted_1 = { class: "dawn-page" };
const _hoisted_2 = { class: "dawn-topbar" };
const _hoisted_3 = {
  key: 0,
  class: "text-medium-emphasis pa-6 text-center"
};
const _hoisted_4 = { key: 1 };
const _hoisted_5 = { class: "dawn-title-cell" };
const _hoisted_6 = { class: "dawn-poster" };
const _hoisted_7 = { class: "dawn-poster__ph" };
const _hoisted_8 = {
  key: 1,
  class: "dawn-poster__ph"
};
const _hoisted_9 = { class: "dawn-title-text" };
const _hoisted_10 = { class: "dawn-title-name" };
const _hoisted_11 = {
  key: 0,
  class: "dawn-title-sub"
};
const _hoisted_12 = { class: "dawn-file-cell" };
const _hoisted_13 = { class: "dawn-path-cell" };
const _hoisted_14 = { class: "dawn-path-text" };
const _hoisted_15 = { class: "dawn-footer" };
const _hoisted_16 = { class: "dawn-footer__total" };
const _hoisted_17 = { class: "dawn-footer__pager" };
const _hoisted_18 = {
  key: 1,
  class: "dawn-ellipsis"
};
const _hoisted_19 = {
  key: 2,
  class: "dawn-mobile"
};
const _hoisted_20 = { class: "dawn-mobile-header" };
const _hoisted_21 = { class: "dawn-mobile-poster" };
const _hoisted_22 = { class: "dawn-poster__ph" };
const _hoisted_23 = {
  key: 1,
  class: "dawn-poster__ph"
};
const _hoisted_24 = { class: "dawn-mobile-poster__overlay" };
const _hoisted_25 = { class: "dawn-mobile-titles" };
const _hoisted_26 = { class: "dawn-mobile-title" };
const _hoisted_27 = {
  key: 0,
  class: "dawn-mobile-sub"
};
const _hoisted_28 = { class: "dawn-mobile-meta mt-1" };
const _hoisted_29 = { class: "dawn-mobile-size" };
const _hoisted_30 = { class: "dawn-mobile-time" };
const _hoisted_31 = { class: "dawn-mobile-right" };
const _hoisted_32 = {
  key: 0,
  class: "dawn-mobile-paths"
};
const _hoisted_33 = {
  key: 0,
  class: "dawn-mobile-path-row"
};
const _hoisted_34 = { class: "dawn-path-text" };
const _hoisted_35 = { class: "dawn-mobile-path-row" };
const _hoisted_36 = { class: "dawn-path-text" };
const _hoisted_37 = { class: "dawn-footer" };
const _hoisted_38 = { class: "dawn-footer__total" };
const _hoisted_39 = { class: "dawn-footer__pager" };
const _hoisted_40 = { class: "dawn-mobile-page" };
const {ref,computed,onMounted,watch} = await importShared('vue');

const {useDisplay} = await importShared('vuetify');

const PLUGIN_ID = "OpenListDownloader";
const _sfc_main = /* @__PURE__ */ _defineComponent({
  __name: "AppPage",
  props: {
    navKey: { type: String, default: "main" },
    initialConfig: { type: Object, default: () => ({}) },
    api: { type: Object, default: () => ({}) },
    pluginId: { type: String, default: "" }
  },
  setup(__props) {
    const display = useDisplay();
    const isMobile = computed(() => display.mdAndDown.value);
    const props = __props;
    const loading = ref(false);
    const items = ref([]);
    const error = ref("");
    const search = ref("");
    const page = ref(1);
    const itemsPerPage = ref(50);
    ref([]);
    const headers = [
      { title: "标题", key: "title", sortable: false },
      { title: "文件", key: "file", sortable: false },
      { title: "来源", key: "path", sortable: false },
      { title: "大小", key: "size", sortable: false },
      { title: "时间", key: "time", sortable: false },
      { title: "状态", key: "status", sortable: false },
      { title: "操作", key: "actions", sortable: false }
    ];
    function humanSize(bytes) {
      if (!bytes || bytes <= 0)
        return "-";
      const units = ["B", "KB", "MB", "GB", "TB"];
      let size = Number(bytes);
      let i = 0;
      while (size >= 1024 && i < units.length - 1) {
        size /= 1024;
        i++;
      }
      return `${size.toFixed(i === 0 ? 0 : 2)} ${units[i]}`;
    }
    function cleanPath(url) {
      if (!url)
        return "";
      let u = url;
      try {
        u = decodeURIComponent(u);
      } catch (e) {
      }
      return u.replace(/^https?:\/\/[^/]+\/d\//, "");
    }
    async function loadHistory() {
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
        items.value = (list || []).map((h, idx) => ({
          id: idx,
          poster: h.poster || "",
          title: h.title || "未知",
          year: h.year || "",
          season: h.season || "",
          name: h.name || "",
          download_url: h.download_url || "",
          path_display: cleanPath(h.download_url) || h.name || "",
          media_type: h.media_type || h.type || "文件",
          tag: h.media_type || h.type || "",
          size: humanSize(h.size),
          size_bytes: Number(h.size) || 0,
          time: h.time || "",
          status: h.status || "成功"
        }));
      } catch (e) {
        error.value = String(e?.message || e);
      } finally {
        loading.value = false;
      }
    }
    onMounted(loadHistory);
    watch(search, () => {
      page.value = 1;
    });
    const notice = ref("");
    const noticeType = ref("success");
    const noticeVisible = ref(false);
    let noticeTimer = null;
    function showNotice(msg, type = "success") {
      notice.value = msg;
      noticeType.value = type;
      noticeVisible.value = true;
      if (noticeTimer)
        clearTimeout(noticeTimer);
      noticeTimer = setTimeout(() => {
        noticeVisible.value = false;
      }, 2500);
    }
    async function callPost(path, body) {
      if (props.api && typeof props.api.post === "function") {
        return await props.api.post(`plugin/${PLUGIN_ID}/${path}`, body);
      }
      const res = await fetch(`/api/v1/plugin/${PLUGIN_ID}/${path}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      });
      return await res.json();
    }
    async function redownload(it) {
      try {
        const res = await callPost("redownload", { name: it.name });
        if (res?.success === false) {
          showNotice(res?.message || "重新下载失败", "error");
        } else {
          showNotice("已重新提交下载");
        }
      } catch (e) {
        showNotice(String(e?.message || e), "error");
      }
    }
    async function deleteRecord(it) {
      try {
        const res = await callPost("history/delete", { name: it.name });
        if (res?.success === false) {
          showNotice(res?.message || "删除失败", "error");
        } else {
          items.value = items.value.filter((i) => i.name !== it.name);
          showNotice("已删除该记录");
        }
      } catch (e) {
        showNotice(String(e?.message || e), "error");
      }
    }
    const filtered = computed(() => {
      const kw = search.value.trim().toLowerCase();
      if (!kw)
        return items.value;
      return items.value.filter((it) => {
        return (it.title || "").toLowerCase().includes(kw) || (it.name || "").toLowerCase().includes(kw) || (it.media_type || "").toLowerCase().includes(kw) || (it.tag || "").toLowerCase().includes(kw);
      });
    });
    const totalPages = computed(() => Math.max(1, Math.ceil(filtered.value.length / itemsPerPage.value)));
    const pagedItems = computed(() => {
      const start = (page.value - 1) * itemsPerPage.value;
      return filtered.value.slice(start, start + itemsPerPage.value);
    });
    const rangeText = computed(() => {
      if (!filtered.value.length)
        return "0 - 0 / 0";
      const start = (page.value - 1) * itemsPerPage.value + 1;
      const end = Math.min(start + itemsPerPage.value - 1, filtered.value.length);
      return `${start} - ${end} / ${filtered.value.length}`;
    });
    return (_ctx, _cache) => {
      const _component_v_btn = _resolveComponent("v-btn");
      const _component_v_alert = _resolveComponent("v-alert");
      const _component_v_progress_linear = _resolveComponent("v-progress-linear");
      const _component_v_icon = _resolveComponent("v-icon");
      const _component_v_img = _resolveComponent("v-img");
      const _component_v_chip = _resolveComponent("v-chip");
      const _component_v_list_item = _resolveComponent("v-list-item");
      const _component_v_list = _resolveComponent("v-list");
      const _component_v_menu = _resolveComponent("v-menu");
      const _component_v_table = _resolveComponent("v-table");
      const _component_v_card = _resolveComponent("v-card");
      const _component_v_snackbar = _resolveComponent("v-snackbar");
      return _openBlock(), _createElementBlock("div", _hoisted_1, [
        _createElementVNode("div", _hoisted_2, [
          _createVNode(_component_v_btn, {
            variant: "tonal",
            size: "small",
            "prepend-icon": "mdi-refresh",
            onClick: loadHistory
          }, {
            default: _withCtx(() => [..._cache[5] || (_cache[5] = [
              _createTextVNode(" 刷新 ", -1)
            ])]),
            _: 1
          })
        ]),
        error.value ? (_openBlock(), _createBlock(_component_v_alert, {
          key: 0,
          type: "error",
          variant: "tonal",
          class: "my-3"
        }, {
          default: _withCtx(() => [
            _createTextVNode(_toDisplayString(error.value), 1)
          ]),
          _: 1
        })) : _createCommentVNode("", true),
        loading.value ? (_openBlock(), _createBlock(_component_v_progress_linear, {
          key: 1,
          indeterminate: "",
          color: "primary"
        })) : _createCommentVNode("", true),
        _createVNode(_component_v_card, {
          variant: "flat",
          class: "mt-2"
        }, {
          default: _withCtx(() => [
            !filtered.value.length && !loading.value ? (_openBlock(), _createElementBlock("div", _hoisted_3, " 暂无下载记录 ")) : _createCommentVNode("", true),
            !isMobile.value ? (_openBlock(), _createElementBlock("div", _hoisted_4, [
              _createVNode(_component_v_table, {
                density: "comfortable",
                class: "dawn-table"
              }, {
                default: _withCtx(() => [
                  _createElementVNode("thead", null, [
                    _createElementVNode("tr", null, [
                      (_openBlock(), _createElementBlock(_Fragment, null, _renderList(headers, (h) => {
                        return _createElementVNode("th", {
                          key: h.key,
                          style: _normalizeStyle(h.width ? { width: h.width + "px" } : void 0)
                        }, _toDisplayString(h.title), 5);
                      }), 64))
                    ])
                  ]),
                  _createElementVNode("tbody", null, [
                    (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(pagedItems.value, (it) => {
                      return _openBlock(), _createElementBlock("tr", {
                        key: it.id
                      }, [
                        _createElementVNode("td", null, [
                          _createElementVNode("div", _hoisted_5, [
                            _createElementVNode("div", _hoisted_6, [
                              it.poster ? (_openBlock(), _createBlock(_component_v_img, {
                                key: 0,
                                src: it.poster,
                                cover: ""
                              }, {
                                placeholder: _withCtx(() => [
                                  _createElementVNode("div", _hoisted_7, [
                                    _createVNode(_component_v_icon, {
                                      icon: "mdi-image-off",
                                      size: "18"
                                    })
                                  ])
                                ]),
                                _: 1
                              }, 8, ["src"])) : (_openBlock(), _createElementBlock("div", _hoisted_8, [
                                _createVNode(_component_v_icon, {
                                  icon: "mdi-image-off",
                                  size: "18"
                                })
                              ]))
                            ]),
                            _createElementVNode("div", _hoisted_9, [
                              _createElementVNode("div", _hoisted_10, _toDisplayString(it.title), 1),
                              it.season ? (_openBlock(), _createElementBlock("div", _hoisted_11, "+ 第" + _toDisplayString(it.season) + "季", 1)) : _createCommentVNode("", true)
                            ])
                          ])
                        ]),
                        _createElementVNode("td", null, [
                          _createElementVNode("div", _hoisted_12, _toDisplayString(it.name), 1)
                        ]),
                        _createElementVNode("td", null, [
                          _createElementVNode("div", _hoisted_13, [
                            _createElementVNode("div", _hoisted_14, [
                              _createVNode(_component_v_icon, {
                                icon: "mdi-folder-outline",
                                size: "14",
                                class: "mr-1"
                              }),
                              _createTextVNode(" " + _toDisplayString(it.path_display), 1)
                            ]),
                            it.tag ? (_openBlock(), _createBlock(_component_v_chip, {
                              key: 0,
                              size: "x-small",
                              color: "purple",
                              variant: "tonal",
                              label: "",
                              class: "mt-1"
                            }, {
                              default: _withCtx(() => [
                                _createTextVNode(_toDisplayString(it.tag), 1)
                              ]),
                              _: 2
                            }, 1024)) : _createCommentVNode("", true)
                          ])
                        ]),
                        _createElementVNode("td", null, _toDisplayString(it.size), 1),
                        _createElementVNode("td", null, _toDisplayString(it.time), 1),
                        _createElementVNode("td", null, [
                          _createVNode(_component_v_chip, {
                            size: "x-small",
                            color: "success",
                            variant: "flat",
                            label: ""
                          }, {
                            default: _withCtx(() => [
                              _createTextVNode(_toDisplayString(it.status), 1)
                            ]),
                            _: 2
                          }, 1024)
                        ]),
                        _createElementVNode("td", null, [
                          _createVNode(_component_v_menu, null, {
                            activator: _withCtx(({ props: a }) => [
                              _createVNode(_component_v_btn, _mergeProps({
                                icon: "mdi-dots-vertical",
                                variant: "text",
                                size: "small"
                              }, { ref_for: true }, a), null, 16)
                            ]),
                            default: _withCtx(() => [
                              _createVNode(_component_v_list, { density: "compact" }, {
                                default: _withCtx(() => [
                                  _createVNode(_component_v_list_item, {
                                    "prepend-icon": "mdi-redo",
                                    title: "重新下载",
                                    onClick: ($event) => redownload(it)
                                  }, null, 8, ["onClick"]),
                                  _createVNode(_component_v_list_item, {
                                    "prepend-icon": "mdi-delete-outline",
                                    title: "删除记录",
                                    onClick: ($event) => deleteRecord(it)
                                  }, null, 8, ["onClick"])
                                ]),
                                _: 2
                              }, 1024)
                            ]),
                            _: 2
                          }, 1024)
                        ])
                      ]);
                    }), 128))
                  ])
                ]),
                _: 1
              }),
              _createElementVNode("div", _hoisted_15, [
                _createElementVNode("div", _hoisted_16, _toDisplayString(rangeText.value), 1),
                _createElementVNode("div", _hoisted_17, [
                  _createVNode(_component_v_btn, {
                    icon: "mdi-chevron-left",
                    variant: "text",
                    size: "small",
                    disabled: page.value <= 1,
                    onClick: _cache[0] || (_cache[0] = ($event) => page.value--)
                  }, null, 8, ["disabled"]),
                  (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(totalPages.value, (p) => {
                    return _openBlock(), _createElementBlock(_Fragment, { key: p }, [
                      p === 1 || p === totalPages.value || Math.abs(p - page.value) <= 2 ? (_openBlock(), _createBlock(_component_v_btn, {
                        key: 0,
                        variant: p === page.value ? "flat" : "text",
                        color: p === page.value ? "primary" : void 0,
                        size: "small",
                        onClick: ($event) => page.value = p
                      }, {
                        default: _withCtx(() => [
                          _createTextVNode(_toDisplayString(p), 1)
                        ]),
                        _: 2
                      }, 1032, ["variant", "color", "onClick"])) : p === 2 && page.value > 4 || p === totalPages.value - 1 && page.value < totalPages.value - 3 ? (_openBlock(), _createElementBlock("span", _hoisted_18, "…")) : _createCommentVNode("", true)
                    ], 64);
                  }), 128)),
                  _createVNode(_component_v_btn, {
                    icon: "mdi-chevron-right",
                    variant: "text",
                    size: "small",
                    disabled: page.value >= totalPages.value,
                    onClick: _cache[1] || (_cache[1] = ($event) => page.value++)
                  }, null, 8, ["disabled"])
                ])
              ])
            ])) : isMobile.value ? (_openBlock(), _createElementBlock("div", _hoisted_19, [
              (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(pagedItems.value, (it) => {
                return _openBlock(), _createBlock(_component_v_card, {
                  key: it.id,
                  variant: "flat",
                  class: "dawn-mobile-card mb-3"
                }, {
                  default: _withCtx(() => [
                    _createElementVNode("div", _hoisted_20, [
                      _createElementVNode("div", _hoisted_21, [
                        it.poster ? (_openBlock(), _createBlock(_component_v_img, {
                          key: 0,
                          src: it.poster,
                          cover: ""
                        }, {
                          placeholder: _withCtx(() => [
                            _createElementVNode("div", _hoisted_22, [
                              _createVNode(_component_v_icon, {
                                icon: "mdi-image-off",
                                size: "20"
                              })
                            ])
                          ]),
                          _: 1
                        }, 8, ["src"])) : (_openBlock(), _createElementBlock("div", _hoisted_23, [
                          _createVNode(_component_v_icon, {
                            icon: "mdi-image-off",
                            size: "20"
                          })
                        ])),
                        _createElementVNode("div", _hoisted_24, [
                          _createVNode(_component_v_icon, {
                            icon: "mdi-play",
                            size: "22",
                            color: "white"
                          })
                        ])
                      ]),
                      _createElementVNode("div", _hoisted_25, [
                        _createElementVNode("div", _hoisted_26, _toDisplayString(it.title), 1),
                        it.name !== it.title ? (_openBlock(), _createElementBlock("div", _hoisted_27, _toDisplayString(it.name), 1)) : _createCommentVNode("", true),
                        _createElementVNode("div", _hoisted_28, [
                          it.tag ? (_openBlock(), _createBlock(_component_v_chip, {
                            key: 0,
                            size: "x-small",
                            color: "purple",
                            variant: "tonal",
                            label: ""
                          }, {
                            default: _withCtx(() => [
                              _createTextVNode(_toDisplayString(it.tag), 1)
                            ]),
                            _: 2
                          }, 1024)) : _createCommentVNode("", true),
                          _createElementVNode("span", _hoisted_29, _toDisplayString(it.size), 1),
                          _cache[6] || (_cache[6] = _createElementVNode("span", { class: "dawn-mobile-dot" }, "·", -1)),
                          _createElementVNode("span", _hoisted_30, _toDisplayString(it.time), 1)
                        ])
                      ]),
                      _createElementVNode("div", _hoisted_31, [
                        _createVNode(_component_v_chip, {
                          size: "x-small",
                          color: "success",
                          variant: "flat",
                          label: ""
                        }, {
                          default: _withCtx(() => [
                            _createTextVNode(_toDisplayString(it.status), 1)
                          ]),
                          _: 2
                        }, 1024),
                        _createVNode(_component_v_menu, null, {
                          activator: _withCtx(({ props: a }) => [
                            _createVNode(_component_v_btn, _mergeProps({
                              icon: "mdi-dots-vertical",
                              variant: "text",
                              size: "small"
                            }, { ref_for: true }, a), null, 16)
                          ]),
                          default: _withCtx(() => [
                            _createVNode(_component_v_list, { density: "compact" }, {
                              default: _withCtx(() => [
                                _createVNode(_component_v_list_item, {
                                  "prepend-icon": "mdi-redo",
                                  title: "重新下载",
                                  onClick: ($event) => redownload(it)
                                }, null, 8, ["onClick"]),
                                _createVNode(_component_v_list_item, {
                                  "prepend-icon": "mdi-delete-outline",
                                  title: "删除记录",
                                  onClick: ($event) => deleteRecord(it)
                                }, null, 8, ["onClick"])
                              ]),
                              _: 2
                            }, 1024)
                          ]),
                          _: 2
                        }, 1024)
                      ])
                    ]),
                    it.download_url || it.name ? (_openBlock(), _createElementBlock("div", _hoisted_32, [
                      it.name && it.download_url ? (_openBlock(), _createElementBlock("div", _hoisted_33, [
                        _cache[7] || (_cache[7] = _createElementVNode("span", { class: "dawn-mobile-path-tag" }, "文件", -1)),
                        _createElementVNode("div", _hoisted_34, _toDisplayString(it.name), 1)
                      ])) : _createCommentVNode("", true),
                      _createElementVNode("div", _hoisted_35, [
                        _cache[8] || (_cache[8] = _createElementVNode("span", { class: "dawn-mobile-path-tag" }, "来源", -1)),
                        _createElementVNode("div", _hoisted_36, [
                          _createVNode(_component_v_icon, {
                            icon: "mdi-folder-outline",
                            size: "14",
                            class: "mr-1"
                          }),
                          _createTextVNode(" " + _toDisplayString(it.path_display), 1)
                        ])
                      ])
                    ])) : _createCommentVNode("", true)
                  ]),
                  _: 2
                }, 1024);
              }), 128)),
              _createElementVNode("div", _hoisted_37, [
                _createElementVNode("div", _hoisted_38, _toDisplayString(rangeText.value), 1),
                _createElementVNode("div", _hoisted_39, [
                  _createVNode(_component_v_btn, {
                    icon: "mdi-chevron-left",
                    variant: "text",
                    size: "small",
                    disabled: page.value <= 1,
                    onClick: _cache[2] || (_cache[2] = ($event) => page.value--)
                  }, null, 8, ["disabled"]),
                  _createElementVNode("span", _hoisted_40, _toDisplayString(page.value) + " / " + _toDisplayString(totalPages.value), 1),
                  _createVNode(_component_v_btn, {
                    icon: "mdi-chevron-right",
                    variant: "text",
                    size: "small",
                    disabled: page.value >= totalPages.value,
                    onClick: _cache[3] || (_cache[3] = ($event) => page.value++)
                  }, null, 8, ["disabled"])
                ])
              ])
            ])) : _createCommentVNode("", true)
          ]),
          _: 1
        }),
        _createVNode(_component_v_snackbar, {
          modelValue: noticeVisible.value,
          "onUpdate:modelValue": _cache[4] || (_cache[4] = ($event) => noticeVisible.value = $event),
          color: noticeType.value,
          location: "bottom",
          timeout: "2500"
        }, {
          default: _withCtx(() => [
            _createTextVNode(_toDisplayString(notice.value), 1)
          ]),
          _: 1
        }, 8, ["modelValue", "color"])
      ]);
    };
  }
});

const AppPage_vue_vue_type_style_index_0_scoped_10b6236e_lang = '';

const AppPage = /* @__PURE__ */ _export_sfc(_sfc_main, [["__scopeId", "data-v-10b6236e"]]);

export { AppPage as default };
