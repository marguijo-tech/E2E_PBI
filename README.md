# Purpose

End-To-End project to demonstrate general ETL and data pipeline processes to be delivered in PBI report

## Intent

The purpose of this project is to synthesize disparate data sources related to music, including Spotify—an integral part of my daily life. By combining data from various platforms, I aim to uncover meaningful insights into my listening habits, preferences, and trends. This self-analysis not only offers a deeper understanding of the music I gravitate toward but also serves as a way to explore and showcase data integration and visualization techniques in a personal context.  

## Visualizations

### 1. **Top 5 Tracks**
   - **Description:** Displays your five most played tracks based on total play count.
   - **Purpose:** Highlights your favorite songs and gives insight into your most enjoyed music.
   - **Visual Type:** Bar Chart.
   - **Key Metric:** Total play count per track.

### 2. **Top 5 Artists**
   - **Description:** Lists the artists you listen to the most, ranked by total play count across all their tracks.
   - **Purpose:** Showcases your favorite artists and helps identify trends in your music preferences.
   - **Visual Type:** Horizontal Bar Chart.
   - **Key Metric:** Aggregated play count by artist.

### 3. **Top Listening Trends**
   - **Description:** Illustrates your listening habits over time, showing peaks and troughs in activity.
   - **Purpose:** Provides insights into when you listen to music the most (e.g., seasonal trends or daily patterns).
   - **Visual Type:** Line Chart.
   - **Key Metric:** Play count by time (e.g., month, day, or hour).

### 4. **Total Time Played in Minutes**
   - **Description:** Displays the total duration of all tracks you’ve played, measured in minutes.
   - **Purpose:** Gives a holistic view of your overall engagement with Spotify.
   - **Visual Type:** Single Value Card.
   - **Key Metric:** Sum of track durations in minutes.

### 5. **Collection Max Tempo vs. Average Energy by Artist**
   - **Description:** Compares the maximum tempo of tracks with the average energy for each artist in your collection.
   - **Purpose:** Highlights energetic and upbeat artists in your collection and identifies contrasts in musical style.
   - **Visual Type:** Scatter Plot.
   - **Key Metrics:** 
     - X-Axis: Average Energy (0-1 scale).
     - Y-Axis: Maximum Tempo (BPM).

---

## How to Interpret the Report

- **Discover Your Favorites:** Use the *Top Tracks* and *Top Artists* visuals to find your most listened-to music and artists.
- **Spot Trends:** The *Top Listening Trends* chart helps you understand when you are most active in listening.
- **Understand Your Commitment:** The *Total Time Played* card reveals how much time you’ve invested in your Spotify sessions.
- **Analyze Musical Preferences:** The *Max Tempo vs. Average Energy* scatter plot showcases the balance of pace and energy across your favorite artists.



## Future Enhancements

With additional time and further development, the following enhancements will augment the insights provided by this project and are set to be worked on:

### 1. **Listening Distribution by Day of the Week or Hour**
   - **Visualization:** Heatmap or stacked column chart showing listening frequency by day and hour.
   - **DAX Enhancements:** Create calculated columns for `DayOfWeek` and `Hour`.
   - **Insights:**
     - Identify peak listening times.
     - Analyze day-to-day variance in listening behavior.

### 2. **Comparison of Personal Favorites vs. Global Trends**
   - **Visualization:** Scatterplot or matrix comparing top artists/albums with Kaggle's top 500 albums or Spotify's top tracks dataset.
   - **DAX Enhancements:** Create a calculated column to flag overlaps between personal favorites and global trends.
   - **Insights:**
     - Understand how personal preferences align with global trends.
     - Highlight underappreciated or unique artists in your collection.

### 3. **Average Listening Time Per Track**
   - **Visualization:** Bar chart ranking songs by their average listening duration.
   - **DAX Enhancements:** Calculate average time spent per track.
   - **Insights:**
     - Determine which tracks hold the most attention.
     - Identify standout tracks, whether long epic pieces or short, catchy tunes.

### 4. **Spotify and Discogs Data Overlap**
   - **Visualization:** Venn diagram or stacked bar chart showing the overlap between Spotify playlists and Discogs library.
   - **DAX Enhancements:** Create intersecting measures to determine overlaps.
   - **Insights:**
     - Highlight gaps between digital and physical music collections.
     - Cross-check and ensure your collection's completeness across platforms.

### 5. **Exploration of Genres Over Time**
   - **Visualization:** Stacked area chart showing trends in genre preferences over time.
   - **DAX Enhancements:** Aggregate play counts by genre and time period.
   - **Insights:**
     - Understand shifts in musical taste.
     - Highlight emerging or fading genre interests.

### 6. **Playlist Completeness**
   - **Visualization:** Gauge or progress bar showing the completeness of Discogs wish list versus Spotify playlists.
   - **DAX Enhancements:** Calculate completion percentages.
   - **Insights:**
     - Track progress toward curating complete playlists or wish lists.
     - Identify gaps to prioritize future additions.

### 7. **Rarity of Your Collection**
   - **Visualization:** Highlight unique tracks or albums in your collection (from Discogs) that are least available on Spotify or other platforms.
   - **DAX Enhancements:** Create calculated columns to score rarity.
   - **Insights:**
     - Showcase rare or hidden gems in your collection.
     - Appreciate the uniqueness of lesser-known items.
