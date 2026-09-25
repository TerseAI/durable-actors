# README visualization

[`team-agent.gif`](team-agent.gif) is the existing animation from [`main` at `4bbbb49`](https://github.com/TerseAI/durable-actors/blob/4bbbb49/.github/assets/team-agent.gif), introduced in [PR #75](https://github.com/TerseAI/durable-actors/pull/75). Its image data and playback timing are unchanged: 1280 × 842 pixels, 1,438 frames, and a 57.52-second loop.

[`team-agent.png`](team-agent.png) is a still from 48 seconds into that animation, used when a reader prefers reduced motion. Regenerate it from the repository root with:

```sh
ffmpeg -ss 48 -i .github/assets/team-agent.gif -frames:v 1 .github/assets/team-agent.png
```

Both the repository and SDK READMEs use these assets. The SDK README uses absolute raw GitHub URLs so they also load on npm. The animation illustrates shared agent coordination; it is not a live agent or latency benchmark.
