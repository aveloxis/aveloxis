defp deps do
    [
      {:phoenix, "~> 1.7.0"},
      {:bamboo, git: "https://github.com/pablo-co/bamboo_postmark.git", tag: "v1.0"},
      {:local_dep, path: "../shared"},
      {:burrito, github: "burrito-elixir/burrito"},
      {:ranged, ">= 3.0.0 and < 5.0.0"},
      {:ex_unit_notifier, "~> 1.3", only: :test}
    ]
  end
