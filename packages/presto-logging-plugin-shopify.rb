deb do
    name "presto-logging-plugin-shopify"
    version "1.0"
    release 1
    description "A plugin to log query texts to stdout"
    dependencies []

    build do
        source git: "https://github.com/Shopify/presto-logging-plugin.git", branch: "v#{@version}"
        run "mvn install"
        run "install -d 0700 #{deb_dir}/u/apps/presto/current/plugin/logging-plugin"
        run "install target/logging-plugin-#{@version}.jar #{deb_dir}/u/apps/presto/current/plugin/logging-plugin"
        run "install log-0.139.jar #{deb_dir}/u/apps/presto/current/plugin/logging-plugin"
        run "install event-listener.properties #{deb_dir}/u/apps/presto/current/etc"
    end
end
