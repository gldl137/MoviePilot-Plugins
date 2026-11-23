// vite.config.js
import { defineConfig } from "file:///P:/MoviePilot/MoviePilot-Plugins/plugins.v3/casrestore/node_modules/vite/dist/node/index.js";
import vue from "file:///P:/MoviePilot/MoviePilot-Plugins/plugins.v3/casrestore/node_modules/@vitejs/plugin-vue/dist/index.mjs";
import { federation } from "file:///P:/MoviePilot/MoviePilot-Plugins/plugins.v3/casrestore/node_modules/@module-federation/vite/lib/index.js";
var vite_config_default = defineConfig({
  plugins: [
    vue(),
    federation({
      name: "CasRestore",
      filename: "assets/remoteEntry.js",
      exposes: {
        "./AppPage": "./src/components/AppPage.vue",
        "./Config": "./src/components/Config.vue",
        "./Page": "./src/components/Page.vue"
      },
      dts: false,
      shared: {
        vue: { requiredVersion: false, import: false },
        vuetify: { requiredVersion: false, import: false, singleton: true },
        "vuetify/styles": { requiredVersion: false, import: false, singleton: true }
      }
    })
  ],
  build: {
    target: "esnext",
    minify: false,
    cssCodeSplit: true
  }
});
export {
  vite_config_default as default
};
//# sourceMappingURL=data:application/json;base64,ewogICJ2ZXJzaW9uIjogMywKICAic291cmNlcyI6IFsidml0ZS5jb25maWcuanMiXSwKICAic291cmNlc0NvbnRlbnQiOiBbImNvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9kaXJuYW1lID0gXCJQOlxcXFxNb3ZpZVBpbG90XFxcXE1vdmllUGlsb3QtUGx1Z2luc1xcXFxwbHVnaW5zLnYzXFxcXGNhc3Jlc3RvcmVcIjtjb25zdCBfX3ZpdGVfaW5qZWN0ZWRfb3JpZ2luYWxfZmlsZW5hbWUgPSBcIlA6XFxcXE1vdmllUGlsb3RcXFxcTW92aWVQaWxvdC1QbHVnaW5zXFxcXHBsdWdpbnMudjNcXFxcY2FzcmVzdG9yZVxcXFx2aXRlLmNvbmZpZy5qc1wiO2NvbnN0IF9fdml0ZV9pbmplY3RlZF9vcmlnaW5hbF9pbXBvcnRfbWV0YV91cmwgPSBcImZpbGU6Ly8vUDovTW92aWVQaWxvdC9Nb3ZpZVBpbG90LVBsdWdpbnMvcGx1Z2lucy52My9jYXNyZXN0b3JlL3ZpdGUuY29uZmlnLmpzXCI7aW1wb3J0IHsgZGVmaW5lQ29uZmlnIH0gZnJvbSAndml0ZSdcclxuaW1wb3J0IHZ1ZSBmcm9tICdAdml0ZWpzL3BsdWdpbi12dWUnXHJcbmltcG9ydCB7IGZlZGVyYXRpb24gfSBmcm9tICdAbW9kdWxlLWZlZGVyYXRpb24vdml0ZSdcclxuXHJcbi8vIE1vdmllUGlsb3QgVnVlIFx1ODA1NFx1OTBBNlx1NjNEMlx1NEVGNlx1RkYxQXZ1ZSAvIHZ1ZXRpZnkgXHU3NTMxXHU0RTNCXHU1RTk0XHU3NTI4XHU2Q0U4XHU1MTY1XHVGRjA4aW1wb3J0OmZhbHNlXHVGRjA5XHVGRjBDXHJcbi8vIFx1NjNEMlx1NEVGNlx1NTNFQVx1NjZCNFx1OTczMlx1OEZEQ1x1N0EwQlx1N0VDNFx1NEVGNlx1RkYwQ1x1NTE2NVx1NTNFM1x1NEVBN1x1NzI2OVx1NEUzQSBhc3NldHMvcmVtb3RlRW50cnkuanNcdTMwMDJcclxuZXhwb3J0IGRlZmF1bHQgZGVmaW5lQ29uZmlnKHtcclxuICBwbHVnaW5zOiBbXHJcbiAgICB2dWUoKSxcclxuICAgIGZlZGVyYXRpb24oe1xyXG4gICAgICBuYW1lOiAnQ2FzUmVzdG9yZScsXHJcbiAgICAgIGZpbGVuYW1lOiAnYXNzZXRzL3JlbW90ZUVudHJ5LmpzJyxcclxuICAgICAgZXhwb3Nlczoge1xyXG4gICAgICAgICcuL0FwcFBhZ2UnOiAnLi9zcmMvY29tcG9uZW50cy9BcHBQYWdlLnZ1ZScsXHJcbiAgICAgICAgJy4vQ29uZmlnJzogJy4vc3JjL2NvbXBvbmVudHMvQ29uZmlnLnZ1ZScsXHJcbiAgICAgICAgJy4vUGFnZSc6ICcuL3NyYy9jb21wb25lbnRzL1BhZ2UudnVlJyxcclxuICAgICAgfSxcclxuICAgICAgZHRzOiBmYWxzZSxcclxuICAgICAgc2hhcmVkOiB7XHJcbiAgICAgICAgdnVlOiB7IHJlcXVpcmVkVmVyc2lvbjogZmFsc2UsIGltcG9ydDogZmFsc2UgfSxcclxuICAgICAgICB2dWV0aWZ5OiB7IHJlcXVpcmVkVmVyc2lvbjogZmFsc2UsIGltcG9ydDogZmFsc2UsIHNpbmdsZXRvbjogdHJ1ZSB9LFxyXG4gICAgICAgICd2dWV0aWZ5L3N0eWxlcyc6IHsgcmVxdWlyZWRWZXJzaW9uOiBmYWxzZSwgaW1wb3J0OiBmYWxzZSwgc2luZ2xldG9uOiB0cnVlIH0sXHJcbiAgICAgIH0sXHJcbiAgICB9KSxcclxuICBdLFxyXG4gIGJ1aWxkOiB7XHJcbiAgICB0YXJnZXQ6ICdlc25leHQnLFxyXG4gICAgbWluaWZ5OiBmYWxzZSxcclxuICAgIGNzc0NvZGVTcGxpdDogdHJ1ZSxcclxuICB9LFxyXG59KVxyXG4iXSwKICAibWFwcGluZ3MiOiAiO0FBQThWLFNBQVMsb0JBQW9CO0FBQzNYLE9BQU8sU0FBUztBQUNoQixTQUFTLGtCQUFrQjtBQUkzQixJQUFPLHNCQUFRLGFBQWE7QUFBQSxFQUMxQixTQUFTO0FBQUEsSUFDUCxJQUFJO0FBQUEsSUFDSixXQUFXO0FBQUEsTUFDVCxNQUFNO0FBQUEsTUFDTixVQUFVO0FBQUEsTUFDVixTQUFTO0FBQUEsUUFDUCxhQUFhO0FBQUEsUUFDYixZQUFZO0FBQUEsUUFDWixVQUFVO0FBQUEsTUFDWjtBQUFBLE1BQ0EsS0FBSztBQUFBLE1BQ0wsUUFBUTtBQUFBLFFBQ04sS0FBSyxFQUFFLGlCQUFpQixPQUFPLFFBQVEsTUFBTTtBQUFBLFFBQzdDLFNBQVMsRUFBRSxpQkFBaUIsT0FBTyxRQUFRLE9BQU8sV0FBVyxLQUFLO0FBQUEsUUFDbEUsa0JBQWtCLEVBQUUsaUJBQWlCLE9BQU8sUUFBUSxPQUFPLFdBQVcsS0FBSztBQUFBLE1BQzdFO0FBQUEsSUFDRixDQUFDO0FBQUEsRUFDSDtBQUFBLEVBQ0EsT0FBTztBQUFBLElBQ0wsUUFBUTtBQUFBLElBQ1IsUUFBUTtBQUFBLElBQ1IsY0FBYztBQUFBLEVBQ2hCO0FBQ0YsQ0FBQzsiLAogICJuYW1lcyI6IFtdCn0K
