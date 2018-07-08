#!/usr/bin/env bash
# Youtube RAW Data
#
# luigi --module tasks.channels vtalks.channels.FetchRawYoutubeData --youtube-url https://www.youtube.com/channel/UCSRhwaM00ay0fasnsw6EXKA
# luigi --module tasks.playlists vtalks.playlists.FetchRawYoutubeData --youtube-url https://www.youtube.com/playlist?list=PLMW8Xq7bXrG7XGG29sXso2hYYNW_14s_A
# luigi --module tasks.playlists vtalks.playlists.items.FetchRawYoutubeData --youtube-url https://www.youtube.com/playlist?list=PLMW8Xq7bXrG7XGG29sXso2hYYNW_14s_A
# luigi --module tasks.talks vtalks.talks.FetchRawYoutubeData --youtube-url https://www.youtube.com/watch?v=fV-phU9m9kg

# Workflows
#
# luigi --module tasks.playlists vtalks.playlists.Playlist --youtube-url https://www.youtube.com/playlist?list=PLMW8Xq7bXrG7XGG29sXso2hYYNW_14s_A
luigi --module tasks.talks vtalks.talks.Talk --youtube-url https://www.youtube.com/watch?v=fV-phU9m9kg