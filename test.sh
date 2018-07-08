#!/usr/bin/env bash
# luigi --module tasks.channels vtalks.channels.FetchRawYoutubeData --youtube-url https://www.youtube.com/channel/UCSRhwaM00ay0fasnsw6EXKA
# luigi --module tasks.playlists vtalks.playlists.FetchRawYoutubeData --youtube-url https://www.youtube.com/playlist?list=PLMW8Xq7bXrG7XGG29sXso2hYYNW_14s_A
luigi --module tasks.playlists vtalks.playlists.items.FetchRawYoutubeData --youtube-url https://www.youtube.com/playlist?list=PLMW8Xq7bXrG7XGG29sXso2hYYNW_14s_A

