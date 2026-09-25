# README visualization

`agent-visualization.gif` and `agent-visualization.png` are rendered from the [Team agent visualization](https://github.com/TerseAI/visualizations/tree/94ee1fe6cd887ba9c0c0e5c4c0ccc417a3f31244/agent-visualization), revision `94ee1fe6cd887ba9c0c0e5c4c0ccc417a3f31244`.

The GIF shows the full 23-second source timeline. The opening setup (source seconds 0–6) takes 4.5 seconds. From the first prompt onward, the animation uses the source's default 0.4× speed to keep captions and streamed replies readable. The exported loop lasts 47 seconds at 20 frames per second, with 940 frames. The PNG shows the completed scene and is used for readers who prefer reduced motion. Both assets are 1200 pixels wide. This is an illustration of application coordination, not a live agent or latency benchmark.

To refresh the assets, serve that repository locally and render the `team-agent-visualization` custom element in Chromium with a 1200-pixel viewport and a device scale factor of 1. Wait for the component and `document.fonts.ready`, then capture frames 0 through 939 using this timeline mapping:

```js
const seconds = frame < 90 ? frame / 15 : 6 + (frame - 90) / 50
component.seek(seconds)
```

Encode the PNG frames with FFmpeg:

```sh
ffmpeg -framerate 20 -i frames/%04d.png \
  -filter_complex '[0:v]split[a][b];[a]palettegen=stats_mode=diff[p];[b][p]paletteuse=dither=bayer:bayer_scale=3:diff_mode=rectangle' \
  -loop 0 agent-visualization.gif
```

Capture `seek(19)` for the still image. For the README export, remove page padding and minimum height, hide `.topline` and `.controls` inside the shadow root, and give `.narration` a fixed 120-pixel height, `18px 0` padding, and a 36-pixel heading. Center narration vertically using flexbox. Capture only the custom element. These presentation adjustments omit controls that cannot work inside an image; the source's event order is unchanged.

Update both README image references if the asset filenames change. The SDK README uses absolute raw GitHub URLs so the images also load on npm.
