import { _ as __mf_13, o as __mf_6 } from './_virtual_mf___mfe_internal__CasRestore__mf_owner__258351331282643__loadShare__vue__loadShare__-De8H7wIV.js';
import _sfc_main$1 from './Config-BXYyaREy.js';

/**
 * 插件详情入口（宿主点击插件卡片时加载 ./Page 组件）。
 * 参考 CAS助手：点开插件直接进设置界面——这里直接复用 Config.vue 渲染配置表单；
 * Config 内部在 initialConfig 为空时会自动从 /config 接口拉取已保存配置。
 */

const _sfc_main = {
  __name: 'Page',
  props: {
  api: { type: Object, default: () => ({}) },
  pluginId: { type: String, default: '' },
  sourcePluginId: { type: String, default: '' },
  nativeSubscribe: { type: Function, default: null },
},
  setup(__props) {

const props = __props;

return (_ctx, _cache) => {
  return (__mf_13(), __mf_6(_sfc_main$1, {
    api: props.api,
    "plugin-id": props.pluginId
  }, null, 8, ["api", "plugin-id"]))
}
}

};

export { _sfc_main as default };
