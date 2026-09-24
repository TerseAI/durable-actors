# README visualization

`durable-actors.gif` and `durable-actors.png` are rendered from the [Durable Actors visualization](https://github.com/TerseAI/visualizations/tree/fac49079d5aa907f90d4185e819e51ea24c9474d/durable-actors), revision `fac49079d5aa907f90d4185e819e51ea24c9474d`.

The GIF preserves the full 36-second timeline at the source's default half speed: 72 seconds, 10 frames per second, looping. The PNG shows the completed scene and is used for readers who prefer reduced motion. Both are 1000 pixels wide. The diagram is an illustration of coordination, not a latency benchmark.

To refresh the assets, serve that repository locally and render the `durable-actors-visualization` custom element in Chromium. Call `seek(frame / 20)` for frames 0 through 719, capture each frame, and encode them at 10 fps with FFmpeg using a generated 256-color palette. Capture `seek(31)` for the still image.

For the README export, remove page padding and the page's footer note, hide `.topline` and `.playback-controls` inside the shadow root, and give `.narration` a fixed 120-pixel height, `18px 28px` padding, and a 32-pixel heading. Capture only the custom element. These presentation adjustments keep the diagram and narration while omitting controls that cannot work inside an image; the source choreography is unchanged.

Update both README image references if the asset filenames change. The SDK README uses absolute raw GitHub URLs so the images also load on npm.
