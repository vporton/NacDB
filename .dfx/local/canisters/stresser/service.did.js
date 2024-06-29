export const idlFactory = ({ IDL }) => {
  return IDL.Service({ 'main' : IDL.Func([], [], []) });
};
export const init = ({ IDL }) => { return []; };
