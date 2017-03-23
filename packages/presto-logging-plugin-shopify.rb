deb do
    name "presto-logging-plugin-shopify"
    version "1.3"
    release 1
    description "A plugin to log query texts to stdout"
    dependencies []

    build do
        source git: "https://github.com/Shopify/presto-logging-plugin.git", branch: "v#{@version}"
        run "apt-get install -y software-properties-common"
        run "add-apt-repository 'deb http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main'"
        run "apt-get update"
        run "echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections"
        run "apt-get -y install oracle-java8-installer maven"
        run "mvn install"
        run "install -d 0700 #{deb_dir}/u/apps/presto/current/plugin/logging-plugin"
        run "install target/logging-plugin-#{@version}.jar #{deb_dir}/u/apps/presto/current/plugin/logging-plugin"
        run "install log-0.139.jar #{deb_dir}/u/apps/presto/current/plugin/logging-plugin"
        run "install -d 0700 #{deb_dir}/u/apps/presto/etc"
        run "install event-listener.properties #{deb_dir}/u/apps/presto/etc/event-listener.properties"
    end
end
