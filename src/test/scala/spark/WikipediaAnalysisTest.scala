package spark

import org.scalatest.flatspec.AnyFlatSpec

class WikipediaAnalysisTest extends AnyFlatSpec {
  it should "return an array of most commonly used words" in {
    val result = WikipediaAnalysis.findMostUsedWords("src/test/resources/test-data")
    assert(result.deep == Array(("five", 8L), ("four", 7L), ("two", 5L), ("one", 3L), ("six", 2L), ("three", 1)).deep)
  }

  it should "return an array of most changed pages" in {
    val result = WikipediaAnalysis.findMostChangedArticles("src/test/resources/test-data")
    assert(result.deep == Array(("Anarchism", 2), ("AccessibleComputing", 1)).deep)
  }

  it should "return an array of articles with the largest text" in {
    val result = WikipediaAnalysis.findLargestArticles("src/test/resources/test-data")
    assert(result.deep == Array(("Anarchism", 57), ("AccessibleComputing", 33)).deep)
  }
}
