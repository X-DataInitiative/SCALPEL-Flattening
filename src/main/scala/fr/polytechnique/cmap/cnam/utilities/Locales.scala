package fr.polytechnique.cmap.cnam.utilities

import java.util.{Locale, TimeZone}

trait Locales {
  Locale.setDefault(Locale.US)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
}
