import { _ as __mf_12, a as __mf_7, b as __mf_0 } from './_virtual_mf___mfe_internal__CasRestore__mf_owner__258351331282643__loadShare__vue__loadShare__-NkxU9if2.js';
import { _ as _export_sfc } from './_plugin-vue_export-helper-pcqpp-6-.js';

true&&(function polyfill() {
  const relList = document.createElement("link").relList;
  if (relList && relList.supports && relList.supports("modulepreload")) {
    return;
  }
  for (const link of document.querySelectorAll('link[rel="modulepreload"]')) {
    processPreload(link);
  }
  new MutationObserver((mutations) => {
    for (const mutation of mutations) {
      if (mutation.type !== "childList") {
        continue;
      }
      for (const node of mutation.addedNodes) {
        if (node.tagName === "LINK" && node.rel === "modulepreload")
          processPreload(node);
      }
    }
  }).observe(document, { childList: true, subtree: true });
  function getFetchOpts(link) {
    const fetchOpts = {};
    if (link.integrity) fetchOpts.integrity = link.integrity;
    if (link.referrerPolicy) fetchOpts.referrerPolicy = link.referrerPolicy;
    if (link.crossOrigin === "use-credentials")
      fetchOpts.credentials = "include";
    else if (link.crossOrigin === "anonymous") fetchOpts.credentials = "omit";
    else fetchOpts.credentials = "same-origin";
    return fetchOpts;
  }
  function processPreload(link) {
    if (link.ep)
      return;
    link.ep = true;
    const fetchOpts = getFetchOpts(link);
    fetch(link.href, fetchOpts);
  }
}());

const _sfc_main = {  };

const _hoisted_1 = { class: "pa-4" };

function _sfc_render(_ctx, _cache) {
  return (__mf_12(), __mf_7("div", _hoisted_1, "CasRestore dev preview"))
}
const App = /*#__PURE__*/_export_sfc(_sfc_main, [['render',_sfc_render]]);

// 仅用于本地 dev 预览；远程组件由模块联邦暴露，主应用加载 AppPage/Config/Page。

__mf_0(App).mount('#app');
