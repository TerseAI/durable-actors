# README visualization

`durable-actors.gif` and `durable-actors.png` are rendered from the [Durable Actors visualization](https://github.com/TerseAI/visualizations/tree/fac49079d5aa907f90d4185e819e51ea24c9474d/durable-actors), revision `fac49079d5aa907f90d4185e819e51ea24c9474d`.

The GIF preserves all 720 frames of the source timeline. The opening setup (source seconds 0–11.3) plays at 2.5× speed, taking 4.52 seconds; the edits and replication sequence play at two-thirds speed, giving each beat 50% more time than normal. The loop lasts 41.57 seconds. Frame delays are 20 ms for the opening and alternate between 70 and 80 ms afterward (75 ms on average). The PNG shows the completed scene and is used for readers who prefer reduced motion. Both are 1000 pixels wide. The diagram is an illustration of coordination, not a latency benchmark.

To refresh the assets, serve that repository locally and render the `durable-actors-visualization` custom element in Chromium. Call `seek(frame / 20)` for frames 0 through 719, capture each frame, and encode them at 20 fps with FFmpeg using a generated 256-color palette. Then accelerate the opening and slow the remaining sequence with a lossless timing remux:

```sh
ffmpeg -i normal-speed.gif -c:v copy \
  -bsf:v "setts=ts='if(lt(TS,1130),TS*0.4,floor(452+(TS-1130)*1.5))'" \
  -loop 0 -final_delay 8 durable-actors.gif
```

The timing expression uses the GIF time base of 1/100 second. Capture `seek(31)` for the still image.

For the README export, remove page padding and the page's footer note, hide `.topline` and `.playback-controls` inside the shadow root, and give `.narration` a fixed 120-pixel height, `18px 28px` padding, and a 32-pixel heading. Capture only the custom element. These presentation adjustments keep the diagram and narration while omitting controls that cannot work inside an image; the source choreography is unchanged.

Update both README image references if the asset filenames change. The SDK README uses absolute raw GitHub URLs so the images also load on npm.
