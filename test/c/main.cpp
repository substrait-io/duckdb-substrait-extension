#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

int main(int argc, char* argv[]) {
  // Call Catch2's session to run tests
  return Catch::Session().run(argc, argv);
}
