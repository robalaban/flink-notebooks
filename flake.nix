{
  description = "Java development environment with GraalVM 17";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            jdk17
            gradle
            flink
            hadoop
            python312
            uv
            nodejs
          ];

          shellHook = ''
            export JAVA_HOME=${pkgs.jdk17}
            export PATH=$JAVA_HOME/bin:$PATH
            export HADOOP_HOME=${pkgs.hadoop}
            export PATH=$HADOOP_HOME/bin:$PATH
            export HADOOP_CLASSPATH=$HADOOP_HOME/etc/hadoop:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/yarn/lib/*

            # Install vsce locally if not present
            if [ ! -d "node_modules/@vscode/vsce" ]; then
              echo "Installing @vscode/vsce locally..."
              npm install --no-save @vscode/vsce
            fi

            # Add local node_modules/.bin to PATH for vsce
            export PATH="$PWD/node_modules/.bin:$PATH"
          '';
        };
      });
}
