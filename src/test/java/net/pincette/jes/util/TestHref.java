package net.pincette.jes.util;

import static net.pincette.jes.util.Href.setContextPath;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestHref {
  private final String UUID = "c9e24445-bcad-4435-b9fd-d3f3a165bd50";

  @Test
  @DisplayName("href1")
  void href1() {
    assertEquals(new Href("a", "b", UUID), new Href("/a/b/" + UUID));
  }

  @Test
  @DisplayName("href2")
  void href2() {
    assertEquals(new Href("a", "b"), new Href("/a/b"));
  }

  @Test
  @DisplayName("href3")
  void href3() {
    final Href href = new Href("a", "b", UUID);

    assertEquals("a", href.app);
    assertEquals("a-b", href.type);
    assertEquals(UUID, href.id);
  }

  @Test
  @DisplayName("href4")
  void href4() {
    assertEquals("/a/b/" + UUID, new Href("/a/b/" + UUID).path());
  }

  @Test
  @DisplayName("href5")
  void href5() {
    setContextPath("/api");
    assertEquals(new Href("a", "b", UUID), new Href("/api/a/b/" + UUID));
  }

  @Test
  @DisplayName("href6")
  void href6() {
    assertEquals("/api/a/b/" + UUID, new Href("/api/a/b/" + UUID).path());
  }
}
